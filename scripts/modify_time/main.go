//go:build cgo

package main

/*
#include <stdint.h>
struct iovec {
	intptr_t iov_base;
	size_t iov_len;
};
*/
import "C"

import (
	"XiaochenCui/modify_time/pkg/mapreader"
	"XiaochenCui/modify_time/pkg/ptrace"
	"bytes"
	"debug/elf"
	_ "embed"
	"encoding/binary"
	"fmt"
	"log"
	"runtime"

	"github.com/spf13/cobra"
)

var (
	//go:embed dirty/fake_clock_gettime.o
	imageContent []byte

	threadRetryLimit = 10
)

const (
	textSection       = ".text"
	relocationSection = ".rela.text"
	varLength         = 8
	syscallInstrSize  = 2

	vdsoEntryName = "[vdso]"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	var targetPID int

	rootCmd := cobra.Command{
		Use:   "modify_time",
		Short: "A CLI tool to modify a process's time",
		Run: func(cmd *cobra.Command, args []string) {
			run(targetPID)
		},
	}

	rootCmd.Flags().IntVarP(&targetPID, "pid", "p", 0, "The target process ID")
	rootCmd.MarkFlagRequired("pid")

	rootCmd.Execute()
}

func run(pid int) {
	clockGetTimeImage, err := LoadFakeImageFromEmbedFs()
	if err != nil {
		panic(err)
	}

	// These three consts corresponding to the three extern variables in the fake_clock_gettime.c
	const (
		externVarClockIdsMask = "CLOCK_IDS_MASK"
		externVarTvSecDelta   = "TV_SEC_DELTA"
		externVarTvNsecDelta  = "TV_NSEC_DELTA"
	)

	// err = clockGetTimeImage.AttachToProcess(pid, map[string]uint64{
	// 	externVarClockIdsMask: 10,
	// 	externVarTvSecDelta:   20,
	// 	externVarTvNsecDelta:  30,
	// })

	err = clockGetTimeImage.AttachToProcess(pid, map[string]uint64{})
	if err != nil {
		panic(err)
	}
}

// LoadFakeImageFromEmbedFs builds FakeImage from the embed filesystem. It parses the ELF file and extract the variables from the relocation section, reserves the space for them at the end of content, then calculates and saves offsets as "manually relocation"
func LoadFakeImageFromEmbedFs() (*FakeImage, error) {
	symbolName := "clock_gettime"

	elfFile, err := elf.NewFile(bytes.NewReader(imageContent))
	if err != nil {
		return nil, fmt.Errorf("parse elf file %s", err)
	}

	syms, err := elfFile.Symbols()
	if err != nil {
		return nil, fmt.Errorf("get symbols %s", err)
	}

	var imageContent []byte
	imageOffset := make(map[string]int)

	for i, r := range elfFile.Sections {
		log.Printf("section %d: %s", i, r.Name)
	}

	for _, r := range elfFile.Sections {
		if r.Type == elf.SHT_PROGBITS && r.Name == textSection {
			imageContent, err = r.Data()
			if err != nil {
				return nil, fmt.Errorf("read text section data %s", err)
			}
			break
		}
	}

	for _, r := range elfFile.Sections {
		if r.Type == elf.SHT_RELA && r.Name == relocationSection {
			relaSection, err := r.Data()
			if err != nil {
				return nil, fmt.Errorf("read rela section data %s", err)
			}
			relaSectionReader := bytes.NewReader(relaSection)

			var rela elf.Rela64
			for relaSectionReader.Len() > 0 {
				err := binary.Read(relaSectionReader, elfFile.ByteOrder, &rela)
				if err != nil {
					return nil, fmt.Errorf("read rela section data %s", err)
				}

				symNo := rela.Info >> 32
				if symNo == 0 || symNo > uint64(len(syms)) {
					continue
				}

				// sym := syms[symNo-1]
				// byteorder := elfFile.ByteOrder
				// if elfFile.Machine == elf.EM_X86_64 || elfFile.Machine == elf.EM_AARCH64 {
				// 	log.Printf("------")
				// 	log.Printf("rela.Off: %x, rela.Addend: %d, sym.Name: %s", rela.Off, rela.Addend, sym.Name)
				// 	assetLD(rela, imageOffset, &imageContent, sym, byteorder)
				// 	log.Printf("imageOffset: %v", imageOffset)
				// 	log.Printf("imageContent[rela.Off:rela.Off+4]: %v", imageContent[rela.Off:rela.Off+4])
				// 	log.Printf("imageContent (len: %d): %v", len(imageContent), imageContent)
				// } else {
				// 	return nil, fmt.Errorf("unsupported machine type %s", elfFile.Machine)
				// }
			}

			break
		}
	}
	return NewFakeImage(
		symbolName,
		imageContent,
		imageOffset,
	), nil
}

func assetLD(rela elf.Rela64, imageOffset map[string]int, imageContent *[]byte, sym elf.Symbol, byteorder binary.ByteOrder) {
	// The relocation of a X86 image is like:
	// Relocation section '.rela.text' at offset 0x288 contains 3 entries:
	// Offset          Info           Type           Sym. Value    	Sym. Name + Addend
	// 000000000016  000900000002 R_X86_64_PC32     0000000000000000 CLOCK_IDS_MASK - 4
	// 00000000001f  000a00000002 R_X86_64_PC32     0000000000000008 TV_NSEC_DELTA - 4
	// 00000000002a  000b00000002 R_X86_64_PC32     0000000000000010 TV_SEC_DELTA - 4
	//
	// For example, we need to write the offset of `CLOCK_IDS_MASK` - 4 in 0x16 of the section
	// If we want to put the `CLOCK_IDS_MASK` at the end of the section, it will be
	// len(imageContent) - 4 - 0x16

	imageOffset[sym.Name] = len(*imageContent)
	targetOffset := uint32(len(*imageContent)) - uint32(rela.Off) + uint32(rela.Addend)
	byteorder.PutUint32((*imageContent)[rela.Off:rela.Off+4], targetOffset)

	// TODO: support other length besides uint64 (which is 8 bytes)
	*imageContent = append(*imageContent, make([]byte, varLength)...)
}

func assetLD_2(rela elf.Rela64, imageOffset map[string]int, imageContent *[]byte, sym elf.Symbol, byteorder binary.ByteOrder) {
	// The relocation of a X86 image is like:
	// Relocation section '.rela.text' at offset 0x200 contains 3 entries:
	//   Offset          Info           Type           Sym. Value    Sym. Name + Addend
	// 000000000017  000600000004 R_X86_64_PLT32    0000000000000000 rand - 4
	// 00000000001f  000300000002 R_X86_64_PC32     0000000000000008 .LC1 - 4
	// 00000000002f  000400000002 R_X86_64_PC32     0000000000000000 .LC0 - 4
	//
	// For example, we need to write the offset of `rand` - 4 in 0x17 of the section (PLT32)
	// If we want to put the `rand` at the end of the section, it will be len(imageContent) - 4 - 0x17

	imageOffset[sym.Name] = len(*imageContent)
	targetOffset := uint32(len(*imageContent)) - uint32(rela.Off) + uint32(rela.Addend)
	byteorder.PutUint32((*imageContent)[rela.Off:rela.Off+4], targetOffset)

	// TODO: support other length besides uint64 (which is 8 bytes)
	*imageContent = append(*imageContent, make([]byte, varLength)...)
}

func NewFakeImage(symbolName string, content []byte, offset map[string]int) *FakeImage {
	return &FakeImage{symbolName: symbolName, content: content, offset: offset}
}

// FakeImage introduce the replacement of VDSO ELF entry and customizable variables.
// FakeImage could be constructed by LoadFakeImageFromEmbedFs(), and then used by FakeClockInjector.
type FakeImage struct {
	// symbolName is the name of the symbol to be replaced.
	symbolName string
	// content presents .text section which has been "manually relocation", the address of extern variables have been calculated manually
	content []byte
	// offset stores the table with variable name, and it's address in content.
	// the key presents extern variable name, ths value is the address/offset within the content.
	offset map[string]int
	// OriginFuncCode stores the raw func code like getTimeOfDay & ClockGetTime.
	OriginFuncCode []byte
	// OriginAddress stores the origin address of OriginFuncCode.
	OriginAddress uint64
	// fakeEntry stores the fake entry
	fakeEntry *mapreader.Entry
}

// AttachToProcess would use ptrace to replace the VDSO ELF entry with FakeImage.
// Each item in parameter "variables" needs a corresponding entry in FakeImage.offset.
func (it *FakeImage) AttachToProcess(pid int, variables map[string]uint64) (err error) {
	// if len(variables) != len(it.offset) {
	// 	return fmt.Errorf("fake image: extern variable number not match, variables: %v, offset: %v", variables, it.offset)
	// }

	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
	}()

	program, err := ptrace.Trace(pid)
	if err != nil {
		return fmt.Errorf("ptrace on target process, pid: %d", pid)
	}
	defer func() {
		errDe := program.Detach()
		if errDe != nil {
			log.Fatal(err, "fail to detach program", "pid", program.Pid())
		}
	}()

	vdsoEntry, err := FindVDSOEntry(program)
	if err != nil {
		return fmt.Errorf("PID : %d", pid)
	}

	// for index, entry := range program.Entries {
	// 	log.Printf("entry %d: %s, start: %x, end: %x, privilege: %s, path: %s, length: %d", index, entry.Path, entry.StartAddress, entry.EndAddress, entry.Privilege, entry.Path, entry.EndAddress-entry.StartAddress)
	// }

	// return nil

	fakeEntry, err := it.FindInjectedImage(program, len(variables))
	if err != nil {
		return fmt.Errorf("PID : %d", pid)
	}
	// target process has not been injected yet
	if fakeEntry == nil {
		fakeEntry, err = it.InjectFakeImage(program, vdsoEntry)
		if err != nil {
			return fmt.Errorf("error injecting fake image, pid: %d, err: %v", pid, err)
		}
		defer func() {
			if err != nil {
				errIn := it.TryReWriteFakeImage(program)
				if errIn != nil {
					log.Fatal(errIn, "rewrite fail, recover fail")
				}
				it.OriginFuncCode = nil
				it.OriginAddress = 0
			}
		}()
	}

	for k, v := range variables {
		err = it.SetVarUint64(program, fakeEntry, k, v)

		if err != nil {
			return fmt.Errorf("set %s for time skew, pid: %d", k, pid)
		}
	}

	return
}

func FindVDSOEntry(program *ptrace.TracedProgram) (*mapreader.Entry, error) {
	var vdsoEntry *mapreader.Entry
	for index := range program.Entries {
		// reverse loop is faster
		e := program.Entries[len(program.Entries)-index-1]
		if e.Path == vdsoEntryName {
			vdsoEntry = &e
			break
		}
	}
	if vdsoEntry == nil {
		return nil, fmt.Errorf("vdso entry not found")
	}
	log.Printf("vdso entry found, start: %x, end: %x, privilege: %s, path: %s", vdsoEntry.StartAddress, vdsoEntry.EndAddress, vdsoEntry.Privilege, vdsoEntry.Path)
	return vdsoEntry, nil
}

func (it *FakeImage) SetVarUint64(program *ptrace.TracedProgram, entry *mapreader.Entry, symbol string, value uint64) error {
	if offset, ok := it.offset[symbol]; ok {
		err := program.WriteUint64ToAddr(entry.StartAddress+uint64(offset), value)
		return err
	}

	return fmt.Errorf("symbol not found")
}

// FindInjectedImage find injected image to avoid redundant inject.
func (it *FakeImage) FindInjectedImage(program *ptrace.TracedProgram, varNum int) (*mapreader.Entry, error) {
	log.Print("finding injected image")

	// minus tailing variable part
	// every variable has 8 bytes
	if it.fakeEntry != nil {
		content, err := program.ReadSlice(it.fakeEntry.StartAddress, it.fakeEntry.EndAddress-it.fakeEntry.StartAddress)
		if err != nil {
			log.Print("ReadSlice fail")
			return nil, nil
		}
		if varNum*8 > len(it.content) {
			return nil, fmt.Errorf("variable num bigger than content num")
		}
		contentWithoutVariable := (*content)[:len(it.content)-varNum*varLength]
		expectedContentWithoutVariable := it.content[:len(it.content)-varNum*varLength]
		log.Print("successfully read slice", "content", contentWithoutVariable, "expected content", expectedContentWithoutVariable)

		if bytes.Equal(contentWithoutVariable, expectedContentWithoutVariable) {
			log.Print("slice found")
			return it.fakeEntry, nil
		}
		log.Print("slice not found")
	}
	return nil, nil
}

// InjectFakeImage Usage CheckList:
// When error : TryReWriteFakeImage after InjectFakeImage.
func (it *FakeImage) InjectFakeImage(program *ptrace.TracedProgram,
	vdsoEntry *mapreader.Entry) (*mapreader.Entry, error) {
	log.Printf("start injecting fake image")
	fakeEntry, err := program.MmapSlice(it.content)
	if err != nil {
		return nil, fmt.Errorf("mmap slice")
	}
	it.fakeEntry = fakeEntry

	log.Printf("fake entry found, start: %x, end: %x, privilege: %s, path: %s", fakeEntry.StartAddress, fakeEntry.EndAddress, fakeEntry.Privilege, fakeEntry.Path)
	log.Printf("address range: %d", fakeEntry.EndAddress-fakeEntry.StartAddress)
	log.Printf("vdso entry found, start: %x, end: %x, privilege: %s, path: %s", vdsoEntry.StartAddress, vdsoEntry.EndAddress, vdsoEntry.Privilege, vdsoEntry.Path)
	log.Printf("address range: %d", vdsoEntry.EndAddress-vdsoEntry.StartAddress)

	// no segment fault:
	// it.symbolName = "xiaochen"
	// it.symbolName = "gettimeofday"

	// segment fault:
	// it.symbolName = "__vdso_clock_gettime"

	log.Printf("finding origin %s in vdso", it.symbolName)
	originAddr, size, err := program.FindSymbolInEntry(it.symbolName, vdsoEntry)
	if err != nil {
		return nil, fmt.Errorf("error finding symbol %s, err: %v", it.symbolName, err)
	}
	log.Printf("origin %s found, start: %x, size: %d", it.symbolName, originAddr, size)
	funcBytes, err := program.ReadSlice(originAddr, size)
	if err != nil {
		return nil, fmt.Errorf("read slice failed")
	}
	err = program.JumpToFakeFunc(originAddr, fakeEntry.StartAddress)
	log.Printf("jump to fake addr %x, err: %v", fakeEntry.StartAddress, err)
	if err != nil {
		errIn := it.TryReWriteFakeImage(program)
		if errIn != nil {
			log.Fatal(errIn, "rewrite fail, recover fail")
		}
		return nil, fmt.Errorf("override origin %s", it.symbolName)
	}

	it.OriginFuncCode = *funcBytes
	it.OriginAddress = originAddr
	return fakeEntry, nil
}

func (it *FakeImage) TryReWriteFakeImage(program *ptrace.TracedProgram) error {
	if it.OriginFuncCode != nil {
		err := program.PtraceWriteSlice(it.OriginAddress, it.OriginFuncCode)
		if err != nil {
			return err
		}
		it.OriginFuncCode = nil
		it.OriginAddress = 0
	}
	return nil
}
