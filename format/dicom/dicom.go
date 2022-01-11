package dicom

import (
	// "strconv"
	// "strings"
	"fmt"
	"github.com/wader/fq/format"
	"github.com/wader/fq/format/registry"
	"github.com/wader/fq/pkg/decode"
	"github.com/wader/fq/pkg/scalar"
)

var probeFormat decode.Group

func init() {
	registry.MustRegister(decode.Format{
		Name:        format.DCM,
		Description: "DICOM file",
		Groups:      []string{format.PROBE},
		DecodeFn:    decodeDicom,
		// Dependencies: []decode.Dependency{
			// {Names: []string{format.PROBE}, Group: &probeFormat},
		// },
	})
}

func visit(d *decode.D, parentItemCount int){
	var itemCount = 0
	for !d.End() {
		tagBits := d.PeekBits(32)
		tagString := fmt.Sprintf("x%08X", (tagBits >> 8 & 0x00FF00FF) | (tagBits << 8 & 0xFF00FF00))

		if(tagBits == 0xfeff00e0){
			//Item Tag https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_7.5.html
			d.FieldStruct("", func(d *decode.D) {
				d.FieldRawLen("Item Tag", 32)
				d.FieldU32LE("Item Len")	
				visit(d, itemCount)
				d.FieldRawLen("Item Delim Tag", 32)
				d.FieldU32LE("Item Delim Len")
	
			})
			itemCount++
			continue
		}
		if(tagBits == 0xfeff0de0){
			//Item Delim.
			return
		}
		if(tagBits == 0xfeffdde0){
			//Seq. Delim.
			return
		}


		d.FieldStruct(tagString, func(d *decode.D) {
			d.FieldU16LE("group", scalar.Hex)
			d.FieldU16LE("element", scalar.Hex)
			vr := d.FieldUTF8("vr", 2)
			var l uint64 = 0;
			if(vr == "OB" || vr =="OW" || vr =="OF" || vr =="SQ" || vr =="UT" || vr =="UN"){
				d.RawLen( 16)
				l = d.FieldU32LE("length")
			} else {
				l = d.FieldU16LE("length")
			}

			if(vr == "SQ"){
				// d.FieldRawLen("item start", 32)
				// d.FieldU32LE("item length")
				// visit(d, 0)
				d.FieldArray("items", func(d *decode.D) { 
					// for each item
					visit(d,0)
				})
				d.FieldRawLen("Seq Delim Tag", 32)
				d.FieldU32LE("Seq Delim Len")
			} else {
				d.FieldRawLen("value", int64(l*8))
			}
		})
	}
}

func decodeDicom(d *decode.D, in interface{}) interface{} {
	d.FieldRawLen("File Preamble", 128*8)
	d.FieldUTF8("DICOM Prefix", 4, d.AssertStr("DICM"))
	visit(d,0)
	return nil
}

//  make fq && ./fq d -d dcm test.dcm