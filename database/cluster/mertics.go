package cluster

import (
	"fmt"
	"os"

	"code.byted.org/gopkg/metrics"
)

// var metricsClient
const (
	LEVELINFO    = 3
	LEVELWARNING = 4
	LEVELERROR   = 5
	LEVELFATAL   = 6
)

var (
	metricsClient   *metrics.MetricsClient
	loadServicePSM  string
	metricsTagInfo  = map[string]string{"level": "INFO"}
	metricsTagWarn  = map[string]string{"level": "WARNING"}
	metricsTagError = map[string]string{"level": "ERROR"}
	metricsTagFatal = map[string]string{"level": "CRITICAL"}
	metricsLim      = 3
)

func init() {

	metricsClient = metrics.NewDefaultMetricsClient("toutiao.database.log", true)
	fmt.Fprint(os.Stdout, "Log metrics: toutiao.service.Dbatmanlog."+".throughput")
	metricsClient.DefineCounter("dbatman.count.go", "")
	metricsClient.DefineStore("dbatman.store", "")
	metricsClient.DefineTimer("dbatman.timer", "")
	loadServicePSM = "test.mysql.dbatman"
}
func DoMertics(logLevel int) {
	if metricsClient == nil {
		return
	}
	if logLevel < metricsLim {
		return
	}
	if logLevel == 3 {
		metricsClient.EmitCounter("dbatman.count.go", 1, "", metricsTagInfo)
	} else if logLevel == 4 { // warning
		metricsClient.EmitCounter("dbatman.count.go", 1, "", metricsTagWarn)
	} else if logLevel == 5 { // error
		metricsClient.EmitCounter("dbatman.count.go", 1, "", metricsTagError)
	} else if logLevel == 6 { // fatal
		metricsClient.EmitCounter("dbatman.count.go", 1, "", metricsTagFatal)
	}
}
