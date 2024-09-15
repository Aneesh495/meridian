package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// SelectItem represents an output expression in SELECT clause.
type SelectItem struct {
	Expr  Expr
	Alias string
}

// SortItem represents an ordering clause in ORDER BY.
type SortItem struct {
	Column    string
	Direction SortDirection
}

// SelectStatement encapsulates the parsed abstract syntax tree of a SQL query.
type SelectStatement struct {
	Items    []SelectItem
	From     string
	Where    Expr
	GroupBy  []string
	OrderBy  []SortItem
	Limit    int
	Offset   int
}

// TokenType classifies lexical tokens.
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenIdent
	TokenNumber
	TokenString
	TokenComma
	TokenOperator
	TokenLParen
	TokenRParen
)

type Token struct {
	Type  TokenType
	Value string
}

// Tokenizer splits SQL text into a stream of tokens.
type Tokenizer struct {
	input []rune
	pos   int
}

func NewTokenizer(sql string) *Tokenizer {
	return &Tokenizer{input: []rune(sql)}
}

func (t *Tokenizer) NextToken() Token {
	for t.pos < len(t.input) && unicode.IsSpace(t.input[t.pos]) {
		t.pos++
	}

	if t.pos >= len(t.input) {
		return Token{Type: TokenEOF}
	}

	ch := t.input[t.pos]

	if ch == ',' {
		t.pos++
		return Token{Type: TokenComma, Value: ","}
	}
	if ch == '(' {
		t.pos++
		return Token{Type: TokenLParen, Value: "("}
	}
	if ch == ')' {
		t.pos++
		return Token{Type: TokenRParen, Value: ")"}
	}

	if ch == '=' || ch == '<' || ch == '>' || ch == '!' || ch == '+' || ch == '-' || ch == '*' || ch == '/' {
		start := t.pos
		t.pos++
		if t.pos < len(t.input) && (t.input[t.pos] == '=' || t.input[t.pos] == '>') {
			t.pos++
		}
		return Token{Type: TokenOperator, Value: string(t.input[start:t.pos])}
	}

	if ch == '\'' || ch == '"' {
		quote := ch
		t.pos++
		start := t.pos
		for t.pos < len(t.input) && t.input[t.pos] != quote {
			t.pos++
		}
		val := string(t.input[start:t.pos])
		if t.pos < len(t.input) {
			t.pos++ // consume closing quote
		}
		return Token{Type: TokenString, Value: val}
	}

	if unicode.IsDigit(ch) {
		start := t.pos
		for t.pos < len(t.input) && (unicode.IsDigit(t.input[t.pos]) || t.input[t.pos] == '.') {
			t.pos++
		}
		return Token{Type: TokenNumber, Value: string(t.input[start:t.pos])}
	}

	if unicode.IsLetter(ch) || ch == '_' {
		start := t.pos
		for t.pos < len(t.input) && (unicode.IsLetter(t.input[t.pos]) || unicode.IsDigit(t.input[t.pos]) || t.input[t.pos] == '_') {
			t.pos++
		}
		return Token{Type: TokenIdent, Value: string(t.input[start:t.pos])}
	}

	t.pos++
	return Token{Type: TokenOperator, Value: string(ch)}
}

// Parser performs recursive-descent parsing on tokenized SQL queries.
type Parser struct {
	tokenizer *Tokenizer
	curr      Token
}

// ParseSQL parses a SQL string into a SelectStatement AST.
func ParseSQL(sql string) (*SelectStatement, error) {
	p := &Parser{tokenizer: NewTokenizer(sql)}
	p.advance()
	return p.parseSelect()
}

func (p *Parser) advance() {
	p.curr = p.tokenizer.NextToken()
}

func (p *Parser) expect(val string) error {
	if strings.ToUpper(p.curr.Value) != strings.ToUpper(val) {
		return fmt.Errorf("expected '%s', got '%s'", val, p.curr.Value)
	}
	p.advance()
	return nil
}

func (p *Parser) parseSelect() (*SelectStatement, error) {
	if err := p.expect("SELECT"); err != nil {
		return nil, err
	}

	stmt := &SelectStatement{}

	// Parse select list
	for {
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		stmt.Items = append(stmt.Items, item)

		if p.curr.Type == TokenComma {
			p.advance()
			continue
		}
		break
	}

	// FROM clause
	if strings.ToUpper(p.curr.Value) == "FROM" {
		p.advance()
		if p.curr.Type != TokenIdent {
			return nil, errors.New("expected table name after FROM")
		}
		stmt.From = p.curr.Value
		p.advance()
	}

	// WHERE clause
	if strings.ToUpper(p.curr.Value) == "WHERE" {
		p.advance()
		whereExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = whereExpr
	}

	// GROUP BY clause
	if strings.ToUpper(p.curr.Value) == "GROUP" {
		p.advance()
		if err := p.expect("BY"); err != nil {
			return nil, err
		}
		for {
			if p.curr.Type != TokenIdent {
				return nil, errors.New("expected column name in GROUP BY")
			}
			stmt.GroupBy = append(stmt.GroupBy, p.curr.Value)
			p.advance()
			if p.curr.Type == TokenComma {
				p.advance()
				continue
			}
			break
		}
	}

	// ORDER BY clause
	if strings.ToUpper(p.curr.Value) == "ORDER" {
		p.advance()
		if err := p.expect("BY"); err != nil {
			return nil, err
		}
		for {
			if p.curr.Type != TokenIdent {
				return nil, errors.New("expected column name in ORDER BY")
			}
			col := p.curr.Value
			p.advance()

			dir := SortAsc
			if strings.ToUpper(p.curr.Value) == "DESC" {
				dir = SortDesc
				p.advance()
			} else if strings.ToUpper(p.curr.Value) == "ASC" {
				p.advance()
			}

			stmt.OrderBy = append(stmt.OrderBy, SortItem{Column: col, Direction: dir})
			if p.curr.Type == TokenComma {
				p.advance()
				continue
			}
			break
		}
	}

	// LIMIT clause
	if strings.ToUpper(p.curr.Value) == "LIMIT" {
		p.advance()
		limitVal, err := strconv.Atoi(p.curr.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT number: %s", p.curr.Value)
		}
		stmt.Limit = limitVal
		p.advance()
	}

	// OFFSET clause
	if strings.ToUpper(p.curr.Value) == "OFFSET" {
		p.advance()
		offVal, err := strconv.Atoi(p.curr.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid OFFSET number: %s", p.curr.Value)
		}
		stmt.Offset = offVal
		p.advance()
	}

	return stmt, nil
}

func (p *Parser) parseSelectItem() (SelectItem, error) {
	expr, err := p.parseExpression()
	if err != nil {
		return SelectItem{}, err
	}

	alias := expr.String()
	if strings.ToUpper(p.curr.Value) == "AS" {
		p.advance()
		if p.curr.Type != TokenIdent {
			return SelectItem{}, errors.New("expected alias after AS")
		}
		alias = p.curr.Value
		p.advance()
	} else if p.curr.Type == TokenIdent && strings.ToUpper(p.curr.Value) != "FROM" {
		alias = p.curr.Value
		p.advance()
	}

	return SelectItem{Expr: expr, Alias: alias}, nil
}

func (p *Parser) parseExpression() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	// Check if followed by binary operator
	if p.curr.Type == TokenOperator || strings.ToUpper(p.curr.Value) == "AND" || strings.ToUpper(p.curr.Value) == "OR" {
		opStr := strings.ToUpper(p.curr.Value)
		p.advance()

		right, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		return &BinaryExpr{
			Left:  left,
			Op:    Op(opStr),
			Right: right,
		}, nil
	}

	return left, nil
}

func (p *Parser) parsePrimary() (Expr, error) {
	if p.curr.Type == TokenNumber {
		valStr := p.curr.Value
		p.advance()
		if strings.Contains(valStr, ".") {
			f, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				return nil, err
			}
			return &LiteralExpr{Value: f, Type: TypeFloat64}, nil
		}
		i, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return nil, err
		}
		return &LiteralExpr{Value: i, Type: TypeInt64}, nil
	}

	if p.curr.Type == TokenString {
		s := p.curr.Value
		p.advance()
		return &LiteralExpr{Value: s, Type: TypeString}, nil
	}

	if p.curr.Type == TokenIdent {
		name := p.curr.Value
		p.advance()
		return &ColumnExpr{Name: name, ColIndex: -1}, nil
	}

	if p.curr.Type == TokenLParen {
		p.advance()
		e, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if p.curr.Type != TokenRParen {
			return nil, errors.New("expected closing parenthesis ')'")
		}
		p.advance()
		return e, nil
	}

	return nil, fmt.Errorf("unexpected token in expression: %s", p.curr.Value)
}
