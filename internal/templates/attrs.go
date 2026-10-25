package templates

import (
	"encoding"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"unicode/utf8"
)

// The browse and compare tables call hxValsJSON and browseURL once or
// twice per row. A map, reflective json.Marshal and url.Values per call
// cost more than the index reads behind a row, so both build their
// output directly for the value types the templates pass. Any other
// shape takes the general encoders, which define the output: the fast
// paths are tested byte-for-byte against them.

// maxFastPairs bounds the key/value pairs the fast paths sort on the
// stack; the templates pass at most seven.
const maxFastPairs = 8

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as encoding/json encodes a string with
// HTML escaping on (the json.Marshal default): <, > and & as \u00XX,
// invalid UTF-8 as �, and U+2028/U+2029 escaped.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b >= ' ' && b != '"' && b != '\\' && b != '<' && b != '>' && b != '&' {
				i++

				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '\\', '"':
				dst = append(dst, '\\', b)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xF])
			}
			i++
			start = i

			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, `�`...)
			i += size
			start = i

			continue
		}
		if c == ' ' || c == ' ' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[c&0xF])
			i += size
			start = i

			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)

	return append(dst, '"')
}

// appendJSONValue appends v as json.Marshal would, for strings (and
// string kinds without their own marshaler), ints and bools. It reports
// false for anything else.
func appendJSONValue(dst []byte, v any) ([]byte, bool) {
	switch v := v.(type) {
	case string:
		return appendJSONString(dst, v), true
	case int:
		return strconv.AppendInt(dst, int64(v), 10), true
	case bool:
		return strconv.AppendBool(dst, v), true
	case json.Marshaler, encoding.TextMarshaler:
		return dst, false
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.String {
		return appendJSONString(dst, rv.String()), true
	}

	return dst, false
}

// sortedPairKeys returns the indexes of pairs' keys in key order. It
// reports false when there are more than maxFastPairs pairs or a key
// repeats, which the general encoders resolve by last-wins.
func sortedPairKeys(order *[maxFastPairs]int, pairs []any) ([]int, bool) {
	n := len(pairs) / 2
	if n > maxFastPairs {
		return nil, false
	}
	idx := order[:n]
	for i := range n {
		key := pairs[2*i].(string) //nolint:forcetypeassert // callers check every key first
		j := i
		for ; j > 0; j-- {
			prev := pairs[idx[j-1]].(string) //nolint:forcetypeassert // as above
			if prev == key {
				return nil, false
			}
			if prev < key {
				break
			}
			idx[j] = idx[j-1]
		}
		idx[j] = 2 * i
	}

	return idx, true
}

// fastHxValsJSON encodes pairs as json.Marshal encodes the same map. It
// reports false when it cannot.
func fastHxValsJSON(pairs []any) (string, bool) {
	var order [maxFastPairs]int
	idx, ok := sortedPairKeys(&order, pairs)
	if !ok {
		return "", false
	}
	var buf [256]byte
	dst := append(buf[:0], '{')
	for i, k := range idx {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, pairs[k].(string)) //nolint:forcetypeassert // callers check every key first
		dst = append(dst, ':')
		if dst, ok = appendJSONValue(dst, pairs[k+1]); !ok {
			return "", false
		}
	}
	dst = append(dst, '}')

	return string(dst), true
}

// queryValue formats v as fmt.Sprint does.
func queryValue(v any) string {
	switch v := v.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	default:
		return fmt.Sprint(v)
	}
}

// fastBrowseURL builds the URL browseURL's url.Values path builds:
// non-empty values, keys sorted, query-escaped. It reports false when
// it cannot.
func fastBrowseURL(pairs []any) (string, bool) {
	var order [maxFastPairs]int
	idx, ok := sortedPairKeys(&order, pairs)
	if !ok {
		return "", false
	}
	var buf [256]byte
	dst := append(buf[:0], "/browse"...)
	sep := byte('?')
	for _, k := range idx {
		v := queryValue(pairs[k+1])
		if v == "" {
			continue
		}
		dst = append(dst, sep)
		sep = '&'
		dst = append(dst, url.QueryEscape(pairs[k].(string))...) //nolint:forcetypeassert // callers check every key first
		dst = append(dst, '=')
		dst = append(dst, url.QueryEscape(v)...)
	}

	return string(dst), true
}
