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

func decodeDicom(d *decode.D, in interface{}) interface{} {
	d.FieldRawLen("File Preamble", 128*8)
	d.FieldUTF8("DICOM Prefix", 4, d.AssertStr("DICM"))

	for i := 0; i < 100; i++{
		tagBits := d.PeekBits(32)
		tagString := fmt.Sprintf("x%08X", (tagBits >> 8 & 0x00FF00FF) | (tagBits << 8 & 0xFF00FF00))
		d.FieldStruct(tagString, func(d *decode.D) {
			d.FieldU16LE("group", scalar.Hex)
			d.FieldU16LE("element", scalar.Hex)
			vr := d.FieldUTF8("VR", 2)
			// d.FieldU16LE("length")
			// d.FieldU32LE("value")
			var l uint64 = 0;
			if(vr == "OB" || vr =="OW" || vr =="OF" || vr =="SQ" || vr =="UT" || vr =="UN"){
				d.FieldRawLen("reserved", 16)
				l = d.FieldU32LE("length")
			} else {
				l = d.FieldU16LE("length")
			}
			d.FieldRawLen("value", int64(l*8))
		})
	}
	// d.FieldU32LE("tag2", scalar.Hex)
	// d.FieldUTF8("VR2", 2)
	// d.FieldU16LE("length2")

	// d.FieldU32LE("tag1", scalar.Hex)
	// d.FieldUTF8("VR", 2)
	// d.FieldU16LE("length")
	// d.FieldU32LE("value")
	// d.FieldU32LE("tag2", scalar.Hex)
	// d.FieldUTF8("VR2", 2)
	// d.FieldU16LE("length2")


	
	// d.FieldArray("tags", func(d *decode.D) {
	// 	// for !d.End() {
	// 	for i := 0; i < 10; i++{
	// 		d.FieldStruct("tag", func(d *decode.D) {


	// 		d.FieldU16LE("tagg", scalar.Hex)
	// 		d.FieldU16LE("tag2", scalar.Hex)
	// 		d.FieldUTF8("VR", 2)
	// 		l := d.FieldU16LE("length")
	// 		d.FieldRawLen("value", int64(l*8))
	// 		})
	// }
// })
	return nil
}

//  make fq && ./fq d -d dcm test.dcm