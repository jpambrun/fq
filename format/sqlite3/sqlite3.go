package sqlite3

// https://www.sqlite.org/fileformat.html
// https://sqlite.org/schematab.html

// TODO: page overflow
// TODO: format version
// TODO: text encoding
// TODO: table/column names
// TODO: assert version and schema version?
// TODO: ptrmap
// TODO: how to represent NULL serials

// CREATE TABLE sqlite_schema(
// 	type text,
// 	name text,
// 	tbl_name text,
// 	rootpage integer,
// 	sql text
// );
// > A table with the name "sqlite_sequence" that is used to keep track of the maximum historical INTEGER PRIMARY KEY for a table using AUTOINCREMENT.
// CREATE TABLE sqlite_sequence(name,seq);
// > Tables with names of the form "sqlite_statN" where N is an integer. Such tables store database statistics gathered by the ANALYZE command and used by the query planner to help determine the best algorithm to use for each query.
// CREATE TABLE sqlite_stat1(tbl,idx,stat);
// Only if compiled with SQLITE_ENABLE_STAT2:
// CREATE TABLE sqlite_stat2(tbl,idx,sampleno,sample);
// Only if compiled with SQLITE_ENABLE_STAT3:
// CREATE TABLE sqlite_stat3(tbl,idx,nEq,nLt,nDLt,sample);
// Only if compiled with SQLITE_ENABLE_STAT4:
// CREATE TABLE sqlite_stat4(tbl,idx,nEq,nLt,nDLt,sample);
// TODO: sqlite_autoindex_TABLE_N index

import (
	"embed"

	"github.com/wader/fq/format"
	"github.com/wader/fq/format/registry"
	"github.com/wader/fq/internal/num"
	"github.com/wader/fq/pkg/decode"
	"github.com/wader/fq/pkg/scalar"
)

//go:embed *.jq
var sqlite3FS embed.FS

func init() {
	registry.MustRegister(decode.Format{
		Name:        format.SQLITE3,
		Description: "SQLite v3 database",
		Groups:      []string{format.PROBE},
		DecodeFn:    sqlite3Decode,
		Files:       sqlite3FS,
	})
}

const (
	bTreeIndexInterior = 0x02
	bTreeTableInterior = 0x05
	bTreeIndexLeaf     = 0x0a
	bTreeTableLeaf     = 0x0d
)

var bTreeTypeMap = scalar.UToScalar{
	bTreeIndexInterior: scalar.S{Sym: "index_interior", Description: "Index interior b-tree page"},
	bTreeTableInterior: scalar.S{Sym: "table_interior", Description: "Table interior b-tree page"},
	bTreeIndexLeaf:     scalar.S{Sym: "index_leaf", Description: "Index leaf b-tree page"},
	bTreeTableLeaf:     scalar.S{Sym: "table_leaf", Description: "Table leaf b-tree page"},
}

const (
	textEncodingUTF8    = 1
	textEncodingUTF16LE = 2
	textEncodingUTF16BE = 3
)

var textEncodingMap = scalar.UToSymStr{
	textEncodingUTF8:    "utf8",
	textEncodingUTF16LE: "utf16le",
	textEncodingUTF16BE: "utf16be",
}

var versionMap = scalar.UToSymStr{
	1: "legacy",
	2: "wal",
}

// TODO: all bits if nine bytes?
// TODO: two complement on bit read count
func varintDecode(d *decode.D) int64 {
	var n uint64
	for i := 0; i < 9; i++ {
		v := d.U8()
		n = n<<7 | v&0b0111_1111
		if v&0b1000_0000 == 0 {
			break
		}
	}
	return num.TwosComplement(64, n)
}

func sqlite3DecodeSerialType(d *decode.D, typ int64) {
	switch typ {
	case 0:
		d.FieldValueStr("value", "NULL", scalar.Description("null"))
	case 1:
		d.FieldS8("value", scalar.Description("8-bit integer"))
	case 2:
		d.FieldS16("value", scalar.Description("16-bit integer"))
	case 3:
		d.FieldS24("value", scalar.Description("24-bit integer"))
	case 4:
		d.FieldS32("value", scalar.Description("32-bit integer"))
	case 5:
		d.FieldS48("value", scalar.Description("48-bit integer"))
	case 6:
		d.FieldS64("value", scalar.Description("64-bit integer"))
	case 7:
		d.FieldF64("value", scalar.Description("64-bit float"))
	case 8:
		d.FieldValueU("value", 0, scalar.Description("constant 0"))
	case 9:
		d.FieldValueU("value", 1, scalar.Description("constant 1"))
	case 10, 11:
	default:
		if typ%2 == 0 {
			// N => 12 and even: (N-12)/2 bytes blob.
			d.FieldRawLen("value", (typ-12)/2*8, scalar.Description("blob"))
		} else {
			// N => 13 and odd: (N-13)/2 bytes text
			d.FieldUTF8("value", int(typ-13)/2, scalar.Description("text"))
		}
	}
}

func sqlite3CellFreeblockDecode(d *decode.D) uint64 {
	nextOffset := d.FieldU16("next_offset")
	if nextOffset == 0 {
		return 0
	}
	// TODO: "header" is size bytes or offset+size? seems to be just size
	// "size of the freeblock in bytes, including the 4-byte header"
	size := d.FieldU16("size")
	d.FieldRawLen("space", int64(size-4)*8)
	return nextOffset
}

func sqlite3CellPayloadDecode(d *decode.D) {
	lengthStart := d.Pos()
	length := d.FieldSFn("length", varintDecode)
	lengtbBits := d.Pos() - lengthStart
	var serialTypes []int64
	d.LenFn((length)*8-lengtbBits, func(d *decode.D) {
		d.FieldArray("serials", func(d *decode.D) {
			for !d.End() {
				serialTypes = append(serialTypes, d.FieldSFn("serial", varintDecode))
			}
		})
	})
	d.FieldArray("contents", func(d *decode.D) {
		for _, s := range serialTypes {
			sqlite3DecodeSerialType(d, s)
		}
	})
}

func sqlite3Decode(d *decode.D, in interface{}) interface{} {
	var pageSizeS *scalar.S
	var databaseSizePages uint64

	d.FieldStruct("header", func(d *decode.D) {
		d.FieldUTF8("magic", 16, d.AssertStr("SQLite format 3\x00"))
		pageSizeS = d.FieldScalarU16("page_size", scalar.UToSymU{1: 65536}) // in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
		d.FieldU8("write_version", versionMap)                              // 1 for legacy; 2 for WAL.
		d.FieldU8("read_version", versionMap)                               // . 1 for legacy; 2 for WAL.
		d.FieldU8("unused_space")                                           // at the end of each page. Usually 0.
		d.FieldU8("maximum_embedded_payload_fraction")                      // . Must be 64.
		d.FieldU8("minimum_embedded_payload_fraction")                      // . Must be 32.
		d.FieldU8("leaf_payload_fraction")                                  // . Must be 32.
		d.FieldU32("file_change_counter")                                   //
		databaseSizePages = d.FieldU32("database_size_pages")               // . The "in-header database size".
		d.FieldU32("page_number_freelist")                                  // of the first freelist trunk page.
		d.FieldU32("total_number_freelist")                                 // pages.
		d.FieldU32("schema_cookie")                                         // .
		d.FieldU32("schema_format_number")                                  // . Supported schema formats are 1, 2, 3, and 4.
		d.FieldU32("default_page_cache_size")                               // .
		d.FieldU32("page_number_largest_root_btree")                        // page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
		d.FieldU32("text_encoding", textEncodingMap)
		d.FieldU32("user_version")                       // " as read and set by the user_version pragma.
		d.FieldU32("incremental_vacuum_mode")            // False (zero) otherwise.
		d.FieldU32("application_id")                     // " set by PRAGMA application_id.
		d.FieldRawLen("reserved", 160, d.BitBufIsZero()) // for expansion. Must be zero.
		d.FieldU32("version_valid_for")                  // number.
		d.FieldU32("sqlite_version_number")              //
	})

	// TODO: nicer API for fallback?
	pageSize := pageSizeS.ActualU()
	if pageSizeS.Sym != nil {
		pageSize = pageSizeS.SymU()
	}

	d.FieldArray("pages", func(d *decode.D) {
		for i := uint64(0); i < databaseSizePages; i++ {
			pageOffset := int64(pageSize) * int64(i)
			d.SeekAbs(pageOffset * 8)
			// skip header for first page
			if i == 0 {
				d.SeekRel(100 * 8)
			}

			d.FieldStruct("page", func(d *decode.D) {
				typ := d.FieldU8("type", bTreeTypeMap)
				startFreeblocks := d.FieldU16("start_freeblocks") // The two-byte integer at offset 1 gives the start of the first freeblock on the page, or is zero if there are no freeblocks.
				pageCells := d.FieldU16("page_cells")             // The two-byte integer at offset 3 gives the number of cells on the page.
				d.FieldU16("cell_start")                          // sThe two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
				d.FieldU8("cell_fragments")                       // The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell content area.
				switch typ {
				case bTreeIndexInterior,
					bTreeTableInterior:
					d.FieldU32("right_pointer") // The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
				}
				var cellPointers []uint64
				d.FieldArray("cells_pointers", func(d *decode.D) {
					for i := uint64(0); i < pageCells; i++ {
						cellPointers = append(cellPointers, d.FieldU16("pointer"))
					}
				})
				if startFreeblocks != 0 {
					d.FieldArray("freeblocks", func(d *decode.D) {
						nextOffset := startFreeblocks
						for nextOffset != 0 {
							d.SeekAbs((pageOffset + int64(nextOffset)) * 8)
							d.FieldStruct("freeblock", func(d *decode.D) {
								nextOffset = sqlite3CellFreeblockDecode(d)
							})
						}
					})
				}
				d.FieldArray("cells", func(d *decode.D) {
					for _, p := range cellPointers {
						d.FieldStruct("cell", func(d *decode.D) {
							// TODO: SeekAbs with fn later?
							d.SeekAbs((pageOffset + int64(p)) * 8)
							switch typ {
							case bTreeIndexInterior:
								d.FieldU32("left_child")
								payLoadLen := d.FieldSFn("payload_len", varintDecode)
								d.LenFn(payLoadLen*8, func(d *decode.D) {
									d.FieldStruct("payload", sqlite3CellPayloadDecode)
								})
							case bTreeTableInterior:
								d.FieldU32("left_child")
								d.FieldSFn("rowid", varintDecode)
							case bTreeIndexLeaf:
								payLoadLen := d.FieldSFn("payload_len", varintDecode)
								d.LenFn(payLoadLen*8, func(d *decode.D) {
									d.FieldStruct("payload", sqlite3CellPayloadDecode)
								})
							case bTreeTableLeaf:
								payLoadLen := d.FieldSFn("payload_len", varintDecode)
								d.FieldSFn("rowid", varintDecode)
								d.LenFn(payLoadLen*8, func(d *decode.D) {
									d.FieldStruct("payload", sqlite3CellPayloadDecode)
								})
							}
						})
					}
				})
			})
		}
	})

	return nil
}
