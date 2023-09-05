package cmd

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/tealeg/xlsx"
	"os"
	"regexp"
	"strconv"
	"strings"
	"vaivhashtool/core"
)

var samplingCmd = &cobra.Command{
	Use:                    "sampling",
	Aliases:                nil,
	SuggestFor:             nil,
	Short:                  "Sampling", // todo
	GroupID:                "",
	Long:                   "", // todo
	Example:                "", // todo
	ValidArgs:              nil,
	ValidArgsFunction:      nil,
	Args:                   nil,
	ArgAliases:             nil,
	BashCompletionFunction: "",
	Deprecated:             "",
	Annotations:            nil,
	Version:                "",
	PersistentPreRun:       nil,
	PersistentPreRunE:      nil,
	PreRun:                 nil,
	PreRunE:                validateSamplingFlags,
	Run:                    nil,
	RunE:                   samplingFunc,
	PostRun:                nil,
	PostRunE:               nil,
	PersistentPostRun:      nil,
	PersistentPostRunE:     nil,
	FParseErrWhitelist:     cobra.FParseErrWhitelist{},
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd:   false,
		DisableNoDescFlag:   false,
		DisableDescriptions: false,
		HiddenDefaultCmd:    false,
	},
	TraverseChildren:           false,
	Hidden:                     false,
	SilenceErrors:              false,
	SilenceUsage:               false,
	DisableFlagParsing:         false,
	DisableAutoGenTag:          false,
	DisableFlagsInUseLine:      false,
	DisableSuggestions:         false,
	SuggestionsMinimumDistance: 0,
}

func samplingFunc(cmd *cobra.Command, args []string) error {
	fmt.Printf("Loading given xlsx file...\n")
	x, err := xlsx.OpenFile(_xlsx_path)
	if err != nil {
		return errors.New(fmt.Sprintf("주어진 파일을 여는데 실패함. %s\n", _xlsx_path))
	}
	csvWriter, err := core.NewCSVWriter(_out)
	if err != nil {
		return err
	}
	defer csvWriter.CloseFile()
	if _without_checksum {
		csvWriter.WriteRecord([]string{"no", "sheet_name", "file_name"})
	} else {
		csvWriter.WriteRecord([]string{"no", "sheet_name", "file_name", "checksum_crc32"})
	}
	meta := make([]SheetMeta, len(x.Sheets))
	for i, sheet := range x.Sheets {
		var rowStart, fileCol, checksumCol, _s int
		fmt.Printf("Sheet (%s)의 레코드 시작 위치를 입력하세요. (1~%d) : ", sheet.Name, sheet.MaxRow)
		fmt.Scanln(&rowStart)
		fmt.Printf("아래 샘플 레코드를 참조하여, Sheet (%s)에서 데이터 파일명 컬럼을 입력하세요.\n", sheet.Name)
		exampleCells := sheet.Row(rowStart).Cells
		for j, cell := range exampleCells {
			fmt.Printf("  %d.%s \n", j+1, cell.String())
		}
		fmt.Printf("1에서 %d 사이 컬림 번호 입력 : ", len(exampleCells))
		fmt.Scanln(&fileCol)
		if !_without_checksum {
			fmt.Printf("위의 샘플 레코드를 참조하여, checksum 데이터 컬럼을 입력하세요. : ")
			fmt.Scanln(&checksumCol)
		}

		rowStart = rowStart - 1
		fileCol = fileCol - 1
		checksumCol = checksumCol - 1
		if _samples_per_sheet {
			fmt.Printf("Sheet (%s)의 추출 샘플 데이터 갯수를 입력하세요 (1~%d) : ", sheet.Name, sheet.MaxRow-rowStart-1)
			fmt.Scanln(&_s)
		}
		meta[i] = SheetMeta{
			name:               sheet.Name,
			rowStart:           rowStart,
			length:             sheet.MaxRow,
			fileCol:            fileCol,
			checksumCol:        checksumCol,
			_samples_per_sheet: _s,
		}

	}
	sampler := core.Sampler{}
	count := 0
	for i, m := range meta {
		total := m.length - m.rowStart - 1
		N := getNumSamples(total)
		if _samples_per_sheet {
			N = m._samples_per_sheet
		}
		samples := sampler.GetSamples(N, total)
		m.targets = make([]string, len(samples))
		for j := 0; j < len(m.targets); j++ {
			m.targets[j] = x.Sheets[i].Row(samples[j] + m.rowStart).Cells[m.fileCol].String()
			if !_without_checksum {
				c := x.Sheets[i].Row(samples[j] + m.rowStart).Cells[m.checksumCol].String()
				csvWriter.WriteRecord([]string{strconv.Itoa(count), m.name, m.targets[j], c})
			} else {
				csvWriter.WriteRecord([]string{strconv.Itoa(count), m.name, m.targets[j]})
			}
			count += 1
		}
	}
	fmt.Printf("Wrote samples to %s.\n", _out)
	return nil
}

func validateSamplingFlags(cmd *cobra.Command, args []string) (err error) {
	if _, err := os.Stat(_xlsx_path); err != nil {
		return errors.New(fmt.Sprintf("path 플래그로 주어진 %s는 잘못된 경로 입니다.", _path))
	}
	if _samples == "per-sheet" {
		_samples_per_sheet = true
	} else if _samples_as_integer, err = strconv.Atoi(_samples); err != nil {
		r, err := regexp.Compile("^(100(\\.0{1,2})?|[1-9]?\\d(\\.\\d{1,2})?)%$")
		if err != nil {
			return errors.New("internal error / samples 플래그 관련 내장 정규 표현식 에러")
		} else {
			if matched := r.MatchString(_samples); matched {
				_samples_as_percentage, err = strconv.ParseFloat(strings.TrimSuffix(_samples, "%"), 64)
				if err != nil {
					return errors.New("internal error on parsing `samples` flag to float value")
				}

			} else {
				return errors.New("samples 플래그는 1.임의의 정수  2.0~100%  3.per-sheet 를 입력으로 받습니다.")
			}
		}
	} else {
		return errors.New("samples 플래그는 1.임의의 정수  2.0~100%  3.per-sheet 를 입력으로 받습니다.")
	}
	return nil
}
func init() {
	samplingCmd.Flags().StringVar(&_xlsx_path, "path", "", "")
	samplingCmd.Flags().StringVar(&_samples, "samples", "", "")
	samplingCmd.Flags().StringVar(&_out, "out", "", "")
	samplingCmd.Flags().BoolVar(&_without_checksum, "without-checksum", false, "")
	samplingCmd.MarkFlagRequired("path")
	samplingCmd.MarkFlagRequired("samples")
	samplingCmd.MarkFlagRequired("out")
	rootCmd.AddCommand(samplingCmd)
}

type SheetMeta struct {
	name               string
	rowStart           int
	length             int
	fileCol            int
	checksumCol        int
	targets            []string
	_samples_per_sheet int
}
