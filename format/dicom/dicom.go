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

func visit(d *decode.D){
	var itemCount = 0
	for !d.End() {
		tagBits := d.PeekBits(32)

		if(tagBits == 0xfeff0de0){
			d.FieldRawLen(fmt.Sprintf("item end%d", itemCount), 32)
			d.FieldU32LE(fmt.Sprintf("item end (always 0)%d", itemCount))
			itemCount++
			continue
		}
		if(tagBits == 0xfeffdde0){
			d.FieldRawLen("seq end", 32)
			d.FieldU32LE("seq end (always 0)")
			return
		}
		if(tagBits == 0xfeff00e0){
			d.FieldRawLen(fmt.Sprintf("item start%d", itemCount), 32)
			d.FieldU32LE(fmt.Sprintf("item length%d", itemCount))
			continue
		}

		tagString := fmt.Sprintf("x%08X", (tagBits >> 8 & 0x00FF00FF) | (tagBits << 8 & 0xFF00FF00))
		d.FieldStruct(tagString, func(d *decode.D) {
			d.FieldU16LE("group", scalar.Hex)
			d.FieldU16LE("element", scalar.Hex)
			vr := d.FieldUTF8("VR", 2)
			var l uint64 = 0;
			if(vr == "OB" || vr =="OW" || vr =="OF" || vr =="SQ" || vr =="UT" || vr =="UN"){
				d.FieldRawLen("reserved", 16)
				l = d.FieldU32LE("length")
			} else {
				l = d.FieldU16LE("length")
			}

			if(vr == "SQ"){
				// d.FieldRawLen("item start", 32)
				// d.FieldU32LE("item length")
				visit(d)
			}
			d.FieldRawLen("value", int64(l*8))
		})
	}
}

func decodeDicom(d *decode.D, in interface{}) interface{} {
	d.FieldRawLen("File Preamble", 128*8)
	d.FieldUTF8("DICOM Prefix", 4, d.AssertStr("DICM"))
	visit(d)
	return nil
}

//  make fq && ./fq d -d dcm test.dcm