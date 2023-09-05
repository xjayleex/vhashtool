package cmd

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"vaivhashtool/core"

	"github.com/schollz/progressbar/v2"
	"github.com/spf13/cobra"
)

const MaxReadConcurrency = 4

func validateGenChecksumFlags(cmd *cobra.Command, args []string) (err error) {
	if strings.Compare(_from, "filesystem") != 0 && strings.Compare(_from, "sheet") != 0 {
		return errors.New("`from` 플래그는 filesystem, 혹은 sheet 중 하나의 값이어야 합니다.")
	}
	if _, err := os.Stat(_path); err != nil {
		return errors.New(fmt.Sprintf("path 플래그로 주어진 %s는 잘못된 경로 입니다.", _path))
	}

	if _samples_as_integer, err = strconv.Atoi(_samples); err != nil {
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
				return errors.New("samples 플래그는 1.임의의 정수 2. 0~100% 를 입력으로 받습니다.")
			}
		}
	}

	if _read_concurrency < 1 {
		_read_concurrency = 1
	}

	if _read_concurrency > MaxReadConcurrency {
		_read_concurrency = MaxReadConcurrency
	}

	return nil
}

func genChecksumFunc(cmd *cobra.Command, args []string) error {
	/*csvWriter, err := core.NewCSVWriter(_out)
	defer csvWriter.CloseFile()
	if err != nil {
		return err
	}
	checkGen := core.NewChecksumGenerator()
	sampler := core.Sampler{} */
	// todo fromSheet := func() error {}
	fromFileSystem := func() error {
		csvWriter, err := core.NewCSVWriter(_out)
		defer csvWriter.CloseFile()
		if err != nil {
			return err
		}
		checkGen := core.NewChecksumGenerator()
		sampler := core.Sampler{}
		traverser := core.NewTraverser(_extensionset)
		fileEntries, err := traverser.Do(_path)
		if err != nil {
			// Todo
		}
		numFiles := len(fileEntries)
		N := getNumSamples(numFiles)
		samples := sampler.GetSamples(N, numFiles)
		targets := make([]core.FileInfo, len(samples))
		for i := 0; i < len(targets); i++ {
			targets[i] = fileEntries[samples[i]]
		}
		start := time.Now()
		bar := progressbar.New(len(targets))
		for _, target := range targets {
			bar.Add(1)
			checksum, err := checkGen.Gen(target.FullPath())
			if err != nil {
				// todo
				csvWriter.WriteRecord([]string{target.FullPath(), target.Name, ""})
			} else {
				csvWriter.WriteRecord([]string{target.FullPath(), target.Name, checksum})
			}
		}
		elapsed := time.Since(start)
		fmt.Printf("took %s\n", elapsed)
		return nil
	}
	fromFileSystemWithMultipleThreads := func(concurrency int) error {
		csvWriter, err := core.NewBufferedCSVWriter(1024, _out)
		if err != nil {
			return err
		}
		go csvWriter.Run()

		sampler := core.Sampler{}
		traverser := core.NewTraverser(_extensionset)
		fileEntries, err := traverser.Do(_path)
		if err != nil {
			// Todo
		}
		numFiles := len(fileEntries)
		N := getNumSamples(numFiles)
		samples := sampler.GetSamples(N, numFiles)
		targets := make([]core.FileInfo, len(samples))
		for i := 0; i < len(targets); i++ {
			targets[i] = fileEntries[samples[i]]
		}

		start := time.Now()

		ticket := core.NewFileTicketPub(targets)
		wg := sync.WaitGroup{}
		bar := progressbar.New(len(targets))
		checkGenList := make([]*core.ChecksumGenerator, concurrency)
		for i := 0; i < concurrency; i++ {
			checkGenList[i] = core.NewChecksumGenerator()
		}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(no int) {
				for {
					file, err := ticket.GetTicket()
					if err != nil {
						// done
						wg.Done()
						break
					}
					checkSum, err := checkGenList[no].Gen(file.FullPath())
					if err != nil {
						csvWriter.PushRecord([]string{file.FullPath(), file.Name, ""})
					} else {
						csvWriter.PushRecord([]string{file.FullPath(), file.Name, checkSum})
					}
					bar.Add(1)
				}
			}(i)
		}
		wg.Wait()
		csvWriter.Stop()
		var readTime, calcTime time.Duration
		for _, gen := range checkGenList {
			readTime += gen.ReadTime
			calcTime += gen.CalcTime
		}
		elapsed := time.Since(start)
		fmt.Printf("took %s\n", elapsed)
		fmt.Printf("Read Time : %d ,  CalcTime : %d ...", readTime, calcTime)
		return nil
	}

	if _from == "filesystem" && _read_concurrency == 1 {
		return fromFileSystem()
	} else if _from == "filesystem" && _read_concurrency != 1 {
		return fromFileSystemWithMultipleThreads(_read_concurrency)
	}

	return nil
}

func getNumSamples(total int) int {
	if _samples_as_percentage == 0 && _samples_as_integer == 0 {
		return 1
	}

	if _samples_as_percentage == 0 {
		if _samples_as_integer > total {
			return total
		} else {
			return _samples_as_integer
		}
	} else {
		m := int(float64(total) * _samples_as_percentage / 100)
		if m < 1 {
			return 1
		} else {
			return m
		}
	}

}

var genChecksumCmd = &cobra.Command{
	Use:                    "gen-checksum",
	Aliases:                nil,
	SuggestFor:             nil,
	Short:                  "Generate checksum of the files", // todo
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
	PreRunE:                validateGenChecksumFlags,
	Run:                    nil,
	RunE:                   genChecksumFunc,
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

func init() {
	genChecksumCmd.Flags().StringVar(&_from, "from", "", "") // todo usage
	genChecksumCmd.Flags().StringVar(&_path, "path", "", "") // todo usage
	genChecksumCmd.Flags().StringSliceVar(&_extensionset, "extension-set", []string{"jpg", "obj", "las", "tiff"}, "")
	genChecksumCmd.Flags().StringVar(&_samples, "samples", "1", "")
	genChecksumCmd.Flags().StringVar(&_out, "out", "", "")
	genChecksumCmd.Flags().IntVar(&_read_concurrency, "concurrency", 1, "")
	genChecksumCmd.MarkFlagRequired("from")
	genChecksumCmd.MarkFlagRequired("path")
	rootCmd.AddCommand(genChecksumCmd)
}
