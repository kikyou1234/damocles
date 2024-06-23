package main

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/venus/venus-shared/typeutil"

	"github.com/filecoin-project/venus/venus-devtool/util"
)

var diffCmd = &cli.Command{
	Name:  "diff",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		for _, pair := range util.ChainAPIPairs {
			showDiff(pair.Venus.Type, pair.Lotus.Type)
		}
		return nil
	},
}

type methDiff struct {
	typ  string
	name string
	desc string
}

func showDiff(impl, origin reflect.Type) {
	fmt.Printf("%s <> %s:\n", formatType(impl), formatType(origin))
	implMethods := typeutil.ExportedMethods(impl)
	originMethods := typeutil.ExportedMethods(origin)

	implMap := map[string]int{}
	originMap := map[string]int{}
	diffs := make([]methDiff, 0, len(implMethods)+len(originMethods))

	for ii := range implMethods {
		implMap[implMethods[ii].Name] = ii
	}

	for oi := range originMethods {
		methName := originMethods[oi].Name
		originMap[methName] = oi

		ii, has := implMap[methName]
		if !has {
			//
			diffs = append(diffs, methDiff{
				name: methName,
				typ:  "-",
			})
			continue
		}

		similar, reason := typeutil.Similar(implMethods[ii].Type, originMethods[oi].Type, typeutil.CodecJSON|typeutil.CodecCbor, typeutil.StructFieldsOrdered|typeutil.StructFieldTagsMatch)
		if similar {
			continue
		}

		diffs = append(diffs, methDiff{
			typ:  ">",
			name: methName,
			desc: reason.Error(),
		})
	}

	for ii := range implMethods {
		methName := implMethods[ii].Name
		if _, has := originMap[methName]; !has {
			diffs = append(diffs, methDiff{
				name: methName,
				typ:  "+",
			})
		}
	}

	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].name < diffs[j].name
	})

	for _, d := range diffs {
		if d.desc == "" {
			fmt.Printf("\t%s %s\n", d.typ, d.name)
			continue
		}

		fmt.Printf("\t%s %s %s\n", d.typ, d.name, d.desc)
	}

	fmt.Println()
}
