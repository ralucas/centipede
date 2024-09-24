package cmd

import (
	"github.com/ralucas/centipede/cmd/centipede"
	"github.com/spf13/cobra"
)

func Initialize() *cobra.Command {
	var verbose bool
	var input string
	var output string
	var fields []string
	var validate bool
	var useCustomParser bool

	rootCmd := &cobra.Command{
		Use:          "centipede",
		Short:        "centipede",
		SilenceUsage: false,
		RunE: func(cmd *cobra.Command, args []string) error {
			conf := centipede.Config{
				Validate:        validate,
				Verbose:         verbose,
				UseCustomParser: useCustomParser,
			}
			return centipede.Run(input, output, fields, conf)
		},
	}

	// flags
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose stdout logging (i.e. debug level)")
	rootCmd.Flags().StringVarP(&input, "input", "i", "", "input file")
	rootCmd.Flags().StringVarP(&output, "output", "o", "output.csv", "output csv file")
	rootCmd.Flags().StringSliceVarP(
		&fields,
		"fields",
		"f",
		[]string{"modified", "publisher.name", "publisher.subOrganizationOf.name", "contactPoint.fn", "keyword"},
		"fields to extract from the input for the csv",
	)
	rootCmd.Flags().BoolVarP(&validate, "validate", "d", false, "run check that dataset json objects are valid")
	rootCmd.Flags().BoolVarP(&useCustomParser, "use-custom-parser", "c", false, "use custom parser")

	// required flags
	rootCmd.MarkFlagRequired("input")

	return rootCmd
}
