// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
)

type SymbolizedLabel struct {
	Name, Value uint32
}

type SymbolsTable struct {
	strings    []string
	symbolsMap map[string]uint32
}

// NewSymbolTable returns a symbol table.
func NewSymbolTable() *SymbolsTable {
	return &SymbolsTable{
		// Empty string is required as a first element.
		symbolsMap: map[string]uint32{"": 0},
		strings:    []string{""},
	}
}

func (t *SymbolsTable) Symbolize(str string) uint32 {
	if ref, ok := t.symbolsMap[str]; ok {
		return ref
	}
	ref := uint32(len(t.strings))
	t.strings = append(t.strings, str)
	t.symbolsMap[str] = ref
	return ref
}

func (t *SymbolsTable) Desymbolize(ref uint32) (string, error) {
	idx := int(ref)
	if idx >= len(t.strings) {
		return "", errors.New("symbols ref out of range")
	}
	return t.strings[idx], nil
}

func (t *SymbolsTable) DesymbolizeLabels(b *labels.ScratchBuilder, symbolizedLabels []SymbolizedLabel) (labels.Labels, error) {
	b.Reset()
	for i := 0; i < len(symbolizedLabels); i++ {
		nameRef := symbolizedLabels[i].Name
		valueRef := symbolizedLabels[i].Value
		if int(nameRef) >= len(t.strings) || int(valueRef) >= len(t.strings) {
			return labels.EmptyLabels(), fmt.Errorf("labelRefs %d (name) = %d (value) outside of symbols table (size %d)", nameRef, valueRef, len(t.strings))
		}
		b.Add(t.strings[nameRef], t.strings[valueRef])
	}
	b.Sort()
	return b.Labels(), nil
}
