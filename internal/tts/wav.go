package tts

import (
	"encoding/binary"
	"fmt"
)

// WavToPCM16LE extracts PCM16LE payload from a simple PCM WAVE file.
func WavToPCM16LE(wav []byte) (pcm []byte, sampleRate int, err error) {
	if len(wav) < 44 || string(wav[0:4]) != "RIFF" || string(wav[8:12]) != "WAVE" {
		return nil, 0, fmt.Errorf("not a RIFF/WAVE file")
	}
	i := 12
	var fmtChunk []byte
	var dataChunk []byte
	for i+8 <= len(wav) {
		cid := string(wav[i : i+4])
		size := int(binary.LittleEndian.Uint32(wav[i+4 : i+8]))
		if i+8+size > len(wav) {
			break
		}
		payload := wav[i+8 : i+8+size]
		switch cid {
		case "fmt ":
			fmtChunk = payload
		case "data":
			dataChunk = payload
		}
		if fmtChunk != nil && dataChunk != nil {
			break
		}
		i += 8 + size
		if size&1 == 1 {
			i++
		}
	}
	if fmtChunk == nil || dataChunk == nil {
		return nil, 0, fmt.Errorf("wav missing fmt/data")
	}
	if len(fmtChunk) < 16 {
		return nil, 0, fmt.Errorf("fmt chunk too short")
	}
	audioFormat := binary.LittleEndian.Uint16(fmtChunk[0:2])
	bits := binary.LittleEndian.Uint16(fmtChunk[14:16])
	sampleRate = int(binary.LittleEndian.Uint32(fmtChunk[4:8]))
	if audioFormat != 1 || bits != 16 {
		return nil, 0, fmt.Errorf("need PCM16 wav, got format=%d bits=%d", audioFormat, bits)
	}
	if sampleRate == 0 {
		sampleRate = 24000
	}
	return dataChunk, sampleRate, nil
}

// WavSampleRate reads sample rate from a standard PCM WAV header (best-effort).
func WavSampleRate(wav []byte) int {
	if len(wav) < 28 {
		return 24000
	}
	sr := int(binary.LittleEndian.Uint32(wav[24:28]))
	if sr == 0 {
		return 24000
	}
	return sr
}
