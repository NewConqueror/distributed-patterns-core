package tool

import (
	"fmt"
	"math"
	"reflect"
	"strings"
)

// ValidationError represents a schema validation failure that can be fed back to an LLM.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

// Validate validates input arguments against a JSONSchema descriptor.
func Validate(schema JSONSchema, args map[string]any) error {
	// If schema defines no object or properties, anything is valid
	if schema.Type != "" && schema.Type != TypeObject {
		return &ValidationError{
			Message: fmt.Sprintf("Şema hatası: Kök şema 'object' tipinde olmalıdır, '%s' tanımlandı", schema.Type),
		}
	}

	// 1. Check required properties
	for _, reqField := range schema.Required {
		val, exists := args[reqField]
		if !exists || val == nil {
			return &ValidationError{
				Field:   reqField,
				Message: fmt.Sprintf("Şema hatası: '%s' alanı eksik. Lütfen bu zorunlu parametreyi sağlayın.", reqField),
			}
		}
	}

	// 2. Validate individual properties
	for fieldName, propSchema := range schema.Properties {
		val, exists := args[fieldName]
		if !exists || val == nil {
			continue // optional and not provided
		}

		if err := validateValue(fieldName, propSchema, val); err != nil {
			return err
		}
	}

	return nil
}

func validateValue(fieldName string, schema JSONSchema, val any) error {
	// Type check
	switch schema.Type {
	case TypeString:
		if _, ok := val.(string); !ok {
			return &ValidationError{
				Field:   fieldName,
				Message: fmt.Sprintf("Şema hatası: '%s' alanı 'string' (metin) tipinde olmalıdır, ancak %T verildi.", fieldName, val),
			}
		}

	case TypeNumber:
		switch val.(type) {
		case float64, float32, int, int64, int32:
		default:
			return &ValidationError{
				Field:   fieldName,
				Message: fmt.Sprintf("Şema hatası: '%s' alanı 'number' (sayı) tipinde olmalıdır, ancak %T verildi.", fieldName, val),
			}
		}

	case TypeInteger:
		switch v := val.(type) {
		case int, int64, int32:
		case float64:
			if math.Trunc(v) != v {
				return &ValidationError{
					Field:   fieldName,
					Message: fmt.Sprintf("Şema hatası: '%s' alanı 'integer' (tam sayı) olmalıdır, ancak kesirli sayı verildi: %v", fieldName, v),
				}
			}
		default:
			return &ValidationError{
				Field:   fieldName,
				Message: fmt.Sprintf("Şema hatası: '%s' alanı 'integer' (tam sayı) tipinde olmalıdır, ancak %T verildi.", fieldName, val),
			}
		}

	case TypeBoolean:
		if _, ok := val.(bool); !ok {
			return &ValidationError{
				Field:   fieldName,
				Message: fmt.Sprintf("Şema hatası: '%s' alanı 'boolean' (true/false) olmalıdır, ancak %T verildi.", fieldName, val),
			}
		}

	case TypeArray:
		rv := reflect.ValueOf(val)
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return &ValidationError{
				Field:   fieldName,
				Message: fmt.Sprintf("Şema hatası: '%s' alanı 'array' (liste) tipinde olmalıdır, ancak %T verildi.", fieldName, val),
			}
		}

		if schema.Items != nil {
			for i := 0; i < rv.Len(); i++ {
				elem := rv.Index(i).Interface()
				elemName := fmt.Sprintf("%s[%d]", fieldName, i)
				if err := validateValue(elemName, *schema.Items, elem); err != nil {
					return err
				}
			}
		}

	case TypeObject:
		nestedMap, ok := val.(map[string]any)
		if !ok {
			return &ValidationError{
				Field:   fieldName,
				Message: fmt.Sprintf("Şema hatası: '%s' alanı 'object' tipinde bir JSON nesnesi olmalıdır, ancak %T verildi.", fieldName, val),
			}
		}
		if err := Validate(schema, nestedMap); err != nil {
			return err
		}
	}

	// Enum check
	if len(schema.Enum) > 0 {
		matched := false
		for _, enumVal := range schema.Enum {
			if fmt.Sprintf("%v", enumVal) == fmt.Sprintf("%v", val) {
				matched = true
				break
			}
		}
		if !matched {
			var allowed []string
			for _, e := range schema.Enum {
				allowed = append(allowed, fmt.Sprintf("%v", e))
			}
			return &ValidationError{
				Field: fieldName,
				Message: fmt.Sprintf("Şema hatası: '%s' alanının değeri '%v' geçerli değil. İzin verilen değerler: [%s]",
					fieldName, val, strings.Join(allowed, ", ")),
			}
		}
	}

	return nil
}
