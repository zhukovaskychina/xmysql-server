package engine

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

const (
	rrfScoreMaxChannels = 32
	rrfScoreMaxConstant = 1_000_000
)

// evaluateReciprocalRankFusion scores one candidate from any number of
// independently ranked SQL result sets. The first argument is the positive
// smoothing constant k; subsequent positive ranks contribute 1/(k+rank).
// NULL ranks represent a candidate absent from that channel and are skipped.
func evaluateReciprocalRankFusion(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) < 2 || len(function.Exprs) > rrfScoreMaxChannels+1 {
		return nil, fmt.Errorf("RRF_SCORE requires k and between 1 and %d channel ranks", rrfScoreMaxChannels)
	}
	arguments := make([]interface{}, 0, len(function.Exprs))
	for _, rawArgument := range function.Exprs {
		argument, ok := rawArgument.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("RRF_SCORE arguments must be scalar expressions")
		}
		value, err := evaluateExpressionWithRow(argument.Expr, values)
		if err != nil {
			return nil, err
		}
		arguments = append(arguments, value)
	}
	if arguments[0] == nil {
		return nil, nil
	}
	k, err := parseRRFPositiveInteger(arguments[0], "k")
	if err != nil {
		return nil, err
	}
	if k == 0 {
		return nil, fmt.Errorf("RRF_SCORE k must be greater than zero")
	}
	if k > rrfScoreMaxConstant {
		return nil, fmt.Errorf("RRF_SCORE k exceeds the maximum %d", rrfScoreMaxConstant)
	}
	var score float64
	contributions := 0
	for channel, rawRank := range arguments[1:] {
		if rawRank == nil {
			continue
		}
		rank, err := parseRRFPositiveInteger(rawRank, fmt.Sprintf("rank %d", channel+1))
		if err != nil {
			return nil, err
		}
		if rank == 0 {
			return nil, fmt.Errorf("RRF_SCORE rank %d must be greater than zero", channel+1)
		}
		contributions++
		score += 1 / (float64(k) + float64(rank))
	}
	if contributions == 0 {
		return nil, nil
	}
	if math.IsInf(score, 0) || math.IsNaN(score) {
		return nil, fmt.Errorf("RRF_SCORE produced a non-finite result")
	}
	return score, nil
}

func parseRRFPositiveInteger(value interface{}, name string) (uint64, error) {
	if parsed, ok := autoIncrementValueAsUint64(value); ok {
		return parsed, nil
	}
	var text string
	switch typed := value.(type) {
	case string:
		text = typed
	case []byte:
		text = string(typed)
	default:
		return 0, fmt.Errorf("RRF_SCORE %s must be a non-negative integer; got %T", name, value)
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(text), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("RRF_SCORE %s must be a non-negative integer: %w", name, err)
	}
	return parsed, nil
}
