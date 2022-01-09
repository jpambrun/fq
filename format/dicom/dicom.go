package dicom

import (
	// "strconv"
	// "strings"

	"github.com/wader/fq/format"
	"github.com/wader/fq/format/registry"
	"github.com/wader/fq/pkg/decode"
	// "github.com/wader/fq/pkg/scalar"
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
	d.FieldRawLen("bs buffer", 128*8)
	d.FieldUTF8("signature", 4, d.AssertStr("DICM"))
	return nil
}
