package chirp

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrInvalidQuery = errors.New("invalid query")
	ErrInvalidField = errors.New("invalid field")
)

// Query represents a parsed SQL-like query
type Query struct {
	Fields   []string          // Fields to select (empty = all)
	Where    []Condition       // WHERE conditions
	Params   map[string]string // Parameter values
}

// Condition represents a WHERE condition
type Condition struct {
	Field    string
	Operator string // =, !=, >, <, >=, <=, LIKE, IN
	Value    interface{}
}

// ParseQuery parses a SQL-like query string
// Example: "SELECT * WHERE field = ?" or "SELECT name, age WHERE age > ?"
func ParseQuery(queryStr string, params map[string]string) (*Query, error) {
	query := &Query{
		Fields: []string{},
		Where:  []Condition{},
		Params: params,
	}

	queryStr = strings.TrimSpace(queryStr)
	upper := strings.ToUpper(queryStr)

	// Parse SELECT clause
	if !strings.HasPrefix(upper, "SELECT") {
		return nil, fmt.Errorf("%w: query must start with SELECT", ErrInvalidQuery)
	}

	// Extract SELECT fields
	selectEnd := strings.Index(upper, " WHERE ")
	if selectEnd == -1 {
		selectEnd = len(queryStr)
	}

	selectPart := strings.TrimSpace(queryStr[6:selectEnd])
	if selectPart == "*" || selectPart == "" {
		query.Fields = []string{} // empty means all fields
	} else {
		fields := strings.Split(selectPart, ",")
		for _, f := range fields {
			query.Fields = append(query.Fields, strings.TrimSpace(f))
		}
	}

	// Parse WHERE clause
	if selectEnd < len(queryStr) {
		wherePart := strings.TrimSpace(queryStr[selectEnd+7:])
		conditions, err := parseWhereClause(wherePart, params)
		if err != nil {
			return nil, err
		}
		query.Where = conditions
	}

	return query, nil
}

// parseWhereClause parses WHERE conditions
// Supports: field = ?, field != ?, field > ?, field < ?, field >= ?, field <= ?, field LIKE ?, field IN (?)
func parseWhereClause(whereStr string, params map[string]string) ([]Condition, error) {
	var conditions []Condition

	// Split by AND
	parts := splitByAnd(whereStr)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		cond, err := parseCondition(part, params)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}

	return conditions, nil
}

func splitByAnd(s string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0

	for i := 0; i < len(s); i++ {
		char := s[i]
		if char == '(' {
			parenDepth++
			current.WriteByte(char)
		} else if char == ')' {
			parenDepth--
			current.WriteByte(char)
		} else if parenDepth == 0 && strings.HasPrefix(strings.ToUpper(s[i:]), " AND ") {
			if current.Len() > 0 {
				parts = append(parts, strings.TrimSpace(current.String()))
				current.Reset()
			}
			i += 4 // skip " AND "
		} else {
			current.WriteByte(char)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}

	return parts
}

func parseCondition(condStr string, params map[string]string) (Condition, error) {
	condStr = strings.TrimSpace(condStr)

	// Try different operators
	operators := []string{"!=", ">=", "<=", "=", ">", "<", " LIKE ", " IN "}
	for _, op := range operators {
		opUpper := strings.ToUpper(op)
		condUpper := strings.ToUpper(condStr)
		if idx := strings.Index(condUpper, opUpper); idx != -1 {
			field := strings.TrimSpace(condStr[:idx])
			valueStr := strings.TrimSpace(condStr[idx+len(op):])

			// Handle parameterized queries (? or :param)
			var value interface{}
			if valueStr == "?" {
				// Try to match by field name first, then use first available param
				if v, ok := params[field]; ok {
					value = v
				} else if len(params) > 0 {
					// Use first available param as fallback
					for _, v := range params {
						value = v
						break
					}
				} else {
					value = ""
				}
			} else if strings.HasPrefix(valueStr, ":") {
				// Named parameter
				paramName := valueStr[1:]
				if v, ok := params[paramName]; ok {
					value = v
				} else {
					return Condition{}, fmt.Errorf("parameter %s not found", paramName)
				}
			} else {
				// Literal value
				value = parseValue(valueStr)
			}

			opClean := strings.TrimSpace(op)
			return Condition{
				Field:    field,
				Operator: opClean,
				Value:    value,
			}, nil
		}
	}

	return Condition{}, fmt.Errorf("%w: unable to parse condition: %s", ErrInvalidQuery, condStr)
}

func parseValue(s string) interface{} {
	s = strings.TrimSpace(s)
	// Remove quotes if present
	if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
		return s[1 : len(s)-1]
	}
	// Try number
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	// Try boolean
	if s == "true" || s == "TRUE" {
		return true
	}
	if s == "false" || s == "FALSE" {
		return false
	}
	return s
}

// Match checks if a JSON document matches the query conditions
func (q *Query) Match(doc map[string]interface{}) bool {
	for _, cond := range q.Where {
		if !q.matchCondition(doc, cond) {
			return false
		}
	}
	return true
}

func (q *Query) matchCondition(doc map[string]interface{}, cond Condition) bool {
	fieldValue, ok := getFieldValue(doc, cond.Field)
	if !ok {
		return false // Field doesn't exist
	}

	switch strings.ToUpper(cond.Operator) {
	case "=":
		return compareValues(fieldValue, cond.Value) == 0
	case "!=":
		return compareValues(fieldValue, cond.Value) != 0
	case ">":
		return compareValues(fieldValue, cond.Value) > 0
	case "<":
		return compareValues(fieldValue, cond.Value) < 0
	case ">=":
		return compareValues(fieldValue, cond.Value) >= 0
	case "<=":
		return compareValues(fieldValue, cond.Value) <= 0
	case "LIKE":
		return matchLike(fieldValue, cond.Value)
	case "IN":
		return matchIn(fieldValue, cond.Value)
	default:
		return false
	}
}

func getFieldValue(doc map[string]interface{}, field string) (interface{}, bool) {
	// Support nested fields with dot notation (e.g., "user.name")
	parts := strings.Split(field, ".")
	current := interface{}(doc)

	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			val, exists := m[part]
			if !exists {
				return nil, false
			}
			current = val
		} else {
			return nil, false
		}
	}

	return current, true
}

func compareValues(a, b interface{}) int {
	// Handle numbers
	if aNum, aOk := toFloat64(a); aOk {
		if bNum, bOk := toFloat64(b); bOk {
			if aNum < bNum {
				return -1
			} else if aNum > bNum {
				return 1
			}
			return 0
		}
	}

	// Handle strings
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	default:
		return 0, false
	}
}

func matchLike(fieldValue interface{}, pattern interface{}) bool {
	fieldStr := fmt.Sprintf("%v", fieldValue)
	patternStr := fmt.Sprintf("%v", pattern)

	// Simple LIKE matching: % = any chars, _ = single char
	// Convert SQL LIKE pattern to regex-like matching
	patternStr = strings.ReplaceAll(patternStr, "%", "*")
	patternStr = strings.ReplaceAll(patternStr, "_", "?")

	// Simple wildcard matching
	if strings.Contains(patternStr, "*") {
		parts := strings.Split(patternStr, "*")
		if len(parts) == 0 {
			return true
		}
		// Check prefix
		if parts[0] != "" && !strings.HasPrefix(fieldStr, parts[0]) {
			return false
		}
		// Check suffix
		if len(parts) > 1 && parts[len(parts)-1] != "" && !strings.HasSuffix(fieldStr, parts[len(parts)-1]) {
			return false
		}
		// Check middle parts
		idx := 0
		for i := 1; i < len(parts)-1; i++ {
			if parts[i] == "" {
				continue
			}
			newIdx := strings.Index(fieldStr[idx:], parts[i])
			if newIdx == -1 {
				return false
			}
			idx += newIdx + len(parts[i])
		}
		return true
	}

	return fieldStr == patternStr
}

func matchIn(fieldValue interface{}, inValue interface{}) bool {
	// Handle array/slice
	if arr, ok := inValue.([]interface{}); ok {
		for _, v := range arr {
			if compareValues(fieldValue, v) == 0 {
				return true
			}
		}
		return false
	}
	// Handle comma-separated string
	if str, ok := inValue.(string); ok {
		parts := strings.Split(str, ",")
		for _, part := range parts {
			if compareValues(fieldValue, strings.TrimSpace(part)) == 0 {
				return true
			}
		}
	}
	return false
}

// Project selects specific fields from a document
func (q *Query) Project(doc map[string]interface{}) map[string]interface{} {
	if len(q.Fields) == 0 {
		return doc // Return all fields
	}

	result := make(map[string]interface{})
	for _, field := range q.Fields {
		if val, ok := getFieldValue(doc, field); ok {
			setFieldValue(result, field, val)
		}
	}
	return result
}

func setFieldValue(doc map[string]interface{}, field string, value interface{}) {
	parts := strings.Split(field, ".")
	current := doc

	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		if next, ok := current[part].(map[string]interface{}); ok {
			current = next
		} else {
			next := make(map[string]interface{})
			current[part] = next
			current = next
		}
	}

	current[parts[len(parts)-1]] = value
}

