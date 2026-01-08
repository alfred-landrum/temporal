package ringpop

import (
	"fmt"
	"math"
	"testing"

	"github.com/twmb/murmur3"
	"go.temporal.io/server/common/convert"
	"gonum.org/v1/gonum/stat"
)

func hostAt(id int) *hostInfo {
	//addr := uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprintf("%d", id))).String()
	//return newHostInfo(addr, nil)
	return newHostInfo(fmt.Sprintf("host%d:%d", id, id), nil)
}

func fakeHosts(count int) (hosts []*hostInfo) {
	for i := range count {
		hosts = append(hosts, hostAt(i))
	}
	return
}

func fakeHostsRange(start, count int) (hosts []*hostInfo) {
	for i := 0; i < count; i++ {
		hosts = append(hosts, hostAt(i+start))
	}
	return
}

type shardHostMapping struct {
	hostMap    map[string]*hostInfo
	shardMap   map[int32]string
	hostCount  map[string]int
	shardCount int32
}

type shardHostMappingDist struct {
	minCount, maxCount int
	mean, stddev       float64
}

func (s shardHostMapping) distributionStats() shardHostMappingDist {
	var c []float64
	var minCount = math.MaxInt
	var maxCount = 0
	for _, cnt := range s.hostCount {
		minCount = min(minCount, cnt)
		maxCount = max(maxCount, cnt)
		c = append(c, float64(cnt))
	}

	mean, stddev := stat.MeanStdDev(c, nil)
	return shardHostMappingDist{
		minCount: minCount,
		maxCount: maxCount,
		mean:     mean,
		stddev:   stddev,
	}
}

func ringpopHostMapping(shardCount int32, hosts []*hostInfo) shardHostMapping {
	hring := newHashRing()

	hostMap := make(map[string]*hostInfo)
	for _, host := range hosts {
		hring.AddMembers(host)
		hostMap[host.GetAddress()] = host
	}

	shardMap := make(map[int32]string)
	for shardID := int32(1); shardID <= shardCount; shardID++ {
		addr, ok := hring.Lookup(convert.Int32ToString(shardID))
		if !ok {
			panic(fmt.Sprintf("failed to lookup host %d", shardID))
		}
		shardMap[shardID] = addr
	}
	hostCount := make(map[string]int)
	for _, addr := range shardMap {
		hostCount[addr]++
	}
	return shardHostMapping{
		hostMap:    hostMap,
		shardMap:   shardMap,
		hostCount:  hostCount,
		shardCount: shardCount,
	}
}

func hrwMapping(shardCount int32, hosts []*hostInfo) shardHostMapping {
	hostMap := make(map[string]*hostInfo)
	for _, host := range hosts {
		addr := host.GetAddress()
		if _, ok := hostMap[addr]; ok {
			panic(fmt.Sprintf("duplicate host address %v", addr))
		}
		hostMap[host.GetAddress()] = host
	}
	bestHostForShardID := func(shardID int32) string {
		var bestHost string
		bestScore := math.Inf(1)
		for addr := range hostMap {
			score := weightedScore(shardID, addr, 1)
			if score < bestScore {
				bestHost = addr
				bestScore = score
			}
		}
		return bestHost
	}

	shardMap := make(map[int32]string)
	for shardID := int32(1); shardID <= shardCount; shardID++ {
		addr := bestHostForShardID(shardID)
		shardMap[shardID] = addr
	}
	hostCount := make(map[string]int)
	for _, addr := range shardMap {
		hostCount[addr]++
	}
	return shardHostMapping{
		hostMap:    hostMap,
		shardMap:   shardMap,
		hostCount:  hostCount,
		shardCount: shardCount,
	}
}

// HRW code taken & lightly modified from MCN repo:
// https://github.com/temporalio/temporal-mcn/blob/main/hrw/hrw.go
func murmur64(seed string) uint64 {
	h := murmur3.New64()
	h.Write([]byte(seed))
	return h.Sum64()
}

func uniform01FromHash64(x uint64) float64 {
	// Map to (0,1) avoiding exact 0 and 1
	return float64(x+1) / float64(math.MaxUint64+2)
}

func weightedScore(shardID int32, addr string, weight uint16) float64 {
	if weight == 0 {
		return math.Inf(1)
	}
	seed := fmt.Sprintf("shardID=%d|addr=%s", shardID, addr)
	u := uniform01FromHash64(murmur64(seed))
	return -math.Log(u) / float64(weight)
}

func shardsMoved(m1, m2 shardHostMapping, shardsMoved map[int32]int) (moved int) {
	if m1.shardCount != m2.shardCount {
		panic(fmt.Sprintf("mapping shard count mismatch: %d vs %d",
			m1.shardCount, m2.shardCount))
	}
	for shardID := int32(1); shardID <= m1.shardCount; shardID++ {
		addr1 := m1.shardMap[shardID]
		addr2 := m2.shardMap[shardID]
		if addr1 != addr2 {
			moved++
			shardsMoved[shardID]++
		}
	}
	return
}

type moveResult struct {
	totalMoves int
	minMoves   int
	maxMoves   int
	avgMoves   float64
}

func studyMove(
	shardCount int32,
	numHosts int,
	mappingMaker func(shardCount int32, hosts []*hostInfo) shardHostMapping,
) moveResult {
	var totalShardsMoved int
	shardsMovedMap := make(map[int32]int)
	hosts := fakeHostsRange(0, numHosts)
	rm := mappingMaker(shardCount, hosts)
	for i := 1; i < numHosts+1; i++ {
		hosts = fakeHostsRange(i, numHosts)
		nextRm := mappingMaker(shardCount, hosts)
		moved := shardsMoved(rm, nextRm, shardsMovedMap)
		totalShardsMoved += moved
		rm = nextRm
	}
	var minMoved = math.MaxInt32
	var maxMoved = 0
	for _, moved := range shardsMovedMap {
		if moved < minMoved {
			minMoved = moved
		}
		if moved > maxMoved {
			maxMoved = moved
		}
	}
	return moveResult{
		totalMoves: totalShardsMoved,
		minMoves:   minMoved,
		maxMoves:   maxMoved,
		avgMoves:   float64(totalShardsMoved) / float64(shardCount),
	}
}

func TestCompareHashing(t *testing.T) {
	testCases := []struct {
		shards int32
		hosts  int
	}{
		{512, 7},
		{2048, 35},
		{4096, 63},
		{16384, 75},
		{16384, 100},
		{16384, 150},
	}
	distTest := func(name string, mappingMaker func(shardCount int32, hosts []*hostInfo) shardHostMapping) {
		for _, tc := range testCases {
			distResult := mappingMaker(tc.shards, fakeHosts(tc.hosts)).distributionStats()
			fmt.Printf("%v %d shards %d hosts => dist min: %d max %d mean %.2f stddev %.2f\n",
				name, tc.shards, tc.hosts, distResult.minCount, distResult.maxCount, distResult.mean, distResult.stddev)
		}
	}

	moveTest := func(name string, mappingMaker func(shardCount int32, hosts []*hostInfo) shardHostMapping) {
		for _, tc := range testCases {
			moveResult := studyMove(tc.shards, tc.hosts, mappingMaker)
			fmt.Printf("%v %d shards %d hosts => moves: total: %d, min: %d, max: %d, avg %.2f\n",
				name, tc.shards, tc.hosts, moveResult.totalMoves, moveResult.minMoves, moveResult.maxMoves, moveResult.avgMoves)
		}
	}

	t.Run("ringpop-dist", func(t *testing.T) {
		distTest("ringpop", ringpopHostMapping)
	})
	t.Run("hrw-dist", func(t *testing.T) {
		distTest("hrw", hrwMapping)
	})
	t.Run("ringpop-move", func(t *testing.T) {
		moveTest("ringpop", ringpopHostMapping)
	})
	t.Run("hrw-move", func(t *testing.T) {
		moveTest("hrw", hrwMapping)
	})
}
