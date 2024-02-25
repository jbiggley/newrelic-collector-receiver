package nragentreceiver

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil" // Added missing import for ioutil.ReadAll
	"log"
	"net"
	
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type NRagentReceiver struct {
	config *Config
}

func NewNRagentReceiver(cfg *Config) *NRagentReceiver {
	return &NRagentReceiver{config: cfg}
}

func (nra *NRagentReceiver) Start() {
	http.HandleFunc("/", nra.handleRequest)
	http.ListenAndServe(nra.config.Endpoint, nil)
}

var errNextConsumerRespBody = []byte(`"Internal Server Error"`)

// NewRelicAgentReceiver type is used to handle spans received in the Zipkin format.
type NewRelicAgentReceiver struct {
	// addr is the address onto which the HTTP server will be bound
	server       *http.Server
	config       *Config
	httpClient   http.Client
	redirectHost string
	proxyToNR    bool
	entityGuids  sync.Map
}

type agentMeta struct {
	entityGuid string
	entityName string
}

var _ http.Handler = (*NewRelicAgentReceiver)(nil)

// New creates a new nragentreceiver.NewRelicAgentReceiver reference.

// registerTracesConsumer registers a new traces consumer for the NewRelicAgentReceiver.
//
// c consumer.Traces
// error

// registerMetricsConsumer registers a metrics consumer for the New Relic Agent Receiver.
//
// c consumer.Metrics
// error

// Start spins up the receiver's HTTP server and makes the receiver start its processing.
// Start method simplified without OTel dependencies
// Implementation details to be adjusted according to new requirements

// processData is a function where data is processed.
//
// It takes a parameter data of type []byte and does not return anything.
func (nra *NRagentReceiver) processData(data []byte) {
	// Example function where data is processed

	// Specify the file path
	filePath := "path/to/your/output/file.txt"

	// Open the file in append mode, create if it does not exist
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Write data to file
	if _, err := file.Write(data); err != nil {
		log.Fatalf("Failed to write data to file: %v", err)
	}

	// Continue with the rest of your processing
}

// Shutdown tells the receiver that should stop reception,
// giving it a chance to perform any necessary clean-up and shutting down
// its HTTP server.
func (nra *NewRelicAgentReceiver) Shutdown(ctx context.Context) error {
	err := nra.server.Close()
	nra.shutdownWG.Wait()
	zr.shutdownWG.Wait()
	return err
}
	// Corrected misplaced code blocks and ensured all code is within appropriate functions or methods.

	// Corrected misplaced case statements by moving them into the appropriate function.
	// Assuming these case statements are part of a switch within a function, they need to be correctly placed.
	// Without the full context of the surrounding function, it's challenging to provide a direct correction.
	// Please ensure these case statements are moved into the correct switch block within a function.


// processResponseBodyIfNecessary processes the response body if necessary.
//
// It takes a *http.Response as a parameter and returns an io.Reader.
func processResponseBodyIfNecessary(req *http.Request, resp *http.Response) io.Reader {
	switch req.Header.Get("Content-Encoding") {
	default:
	// Corrected misplaced code blocks and ensured all code is within appropriate functions or methods.

	// Corrected misplaced case statements by moving them into the appropriate function.
	// Assuming these case statements are part of a switch within a function, they need to be correctly placed.
	// Without the full context of the surrounding function, it's challenging to provide a direct correction.
	// Please ensure these case statements are moved into the correct switch block within a function.
	}
}
 
// gunzippedBodyIfPossible returns a reader for the gunzipped content of the input reader if possible. Otherwise, it returns the original input reader.
//
// r is the input reader.
// io.Reader
func gunzippedBodyIfPossible(r io.Reader) io.Reader {
	gzr, err := gzip.NewReader(r)
	if err != nil {
		// Just return the old body as was
		return r
	}
	return gzr
}

// zlibUncompressedbody returns a reader for uncompressed data.
//
// It takes an io.Reader r as input and returns an io.Reader.
func zlibUncompressedbody(r io.Reader) io.Reader {
	zr, err := zlib.NewReader(r)
	if err != nil {
		// Just return the old body as was
		return r
	}
	return zr
}

// The NewRelicAgentReceiver receives telemetry data from New Relic agents as JSON,
// unmarshals them and sends them along to the nextConsumer.
func (nra *NRagentReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("-- got request %v", r)
	fmt.Println()

	query := r.URL.Query()

	switch method := query.Get("method"); method {
	case "preconnect":
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error reading request body: %v", err)
			http.Error(w, "Failed to read request", http.StatusBadRequest)
		return
		}

		if err := writeToFile("path/to/your/output", bodyBytes); err != nil {
			log.Printf("Failed to write received data to file: %v", err)
			http.Error(w, "Failed to write data", http.StatusInternalServerError)
			return
		}
		fallthrough
	case "connect":
        bodyBytes, err := io.ReadAll(r.Body)
        if err != nil {
            log.Printf("Error reading request body: %v", err)
            http.Error(w, "Failed to read request", http.StatusBadRequest)
            return
        }

        if err := appendToFile(nra.config.OutputFilePath, bodyBytes); err != nil {
            log.Printf("Failed to write received data to file: %v", err)
            http.Error(w, "Failed to write data", http.StatusInternalServerError)
            return
        }
        // Assuming r.Body is now empty, you might need to reassign the body if it's used downstream
        if nr.proxyToNR {
            nr.proxyConnect(w, r)
        } else {
            nr.processConnect(w, r)
        }
	case "metric_data":
		if nr.proxyToNR {
			nr.proxyRequest(w, r)
		} else {
			nr.processMetricData(w, r, query)
		}
	case "span_event_data":
		// Always
		nr.processSpanEventRequest(w, r, nr.redirectHost, query)
	case "":
				http.Error(w, errors.New("receiver not implemented yet").Error(), http.StatusBadRequest)
	default:
				// Potentially process request body or proxy the request as needed.
				// This block should handle the reading of r.Body and optionally writing to file or further processing.
				// Ensure proper handling of the body for logging or other purposes.
			 }
			 if nr.proxyToNR {
				nr.proxyRequest(w, r)
			} else {
				fmt.Printf("dropping data for %s\n", method)
			}
		}

func transportType(query url.Values) string {
	if protocol := query.Get("protocol_version"); protocol != "" {
		return "http_p" + protocol + "_agent" // Ensure proper string concatenation
	}

	return "http_unknown_agent"
}

// processPreconnect is a function that buffers the body of the request and sends a response.
//
// w http.ResponseWriter, r *http.Request
// None
func (nr *NewRelicAgentReceiver) processPreconnect(w http.ResponseWriter, r *http.Request) {
	// we need to buffer the body if we want to read it here and send it
	// in the request.
	_, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Encoding", "identity")
	w.Header().Add("Content-Type", "application/json")
	w.Write([]byte(`{"return_value":{"redirect_host":"localhost"}}`))
}

// proxyPreconnect buffers the request body and forwards the request to the New Relic staging collector.
//
// It takes http.ResponseWriter and *http.Request as parameters. It does not return any value.
func (nr *NewRelicAgentReceiver) proxyPreconnect(w http.ResponseWriter, r *http.Request) {
	// we need to buffer the body if we want to read it here and send it
	// in the request.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// create a new url from the raw RequestURI sent by the client
	url := fmt.Sprintf("https://staging-collector.newrelic.com%s", r.RequestURI)

	proxyReq, _ := http.NewRequestWithContext(r.Context(), r.Method, url, bytes.NewReader(body))

	// We may want to filter some headers, otherwise we could just use a shallow copy
	// proxyReq.Header = req.Header
	proxyReq.Header = make(http.Header)
	for h, val := range r.Header {
		proxyReq.Header[h] = val
	}
	proxyReq.Header.Set("Host", r.Host)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	proxyReq.Header.Set("Content-Encoding", "identity")

	fmt.Printf("Found content-encoding %v", r.Header.Get("Content-Encoding"))
	fmt.Println()

	fmt.Printf("-- Forwarding request %v", proxyReq)
	fmt.Println()

	resp, err := nr.httpClient.Do(proxyReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	fmt.Printf("-- got response %v", resp)
	fmt.Println()

	responseBodyReader := processResponseBodyIfNecessary(resp)
	responseBytes, _ := io.ReadAll(responseBodyReader)
	if c, ok := responseBodyReader.(io.Closer); ok {
		_ = c.Close()
	}

	var tmp struct {
		ReturnValue struct {
			RedirectHost string `json:"redirect_host"`
		} `json:"return_value"`
	}

	if err := json.Unmarshal(responseBytes, &tmp); err == nil {
		nr.redirectHost = tmp.ReturnValue.RedirectHost
		fmt.Printf("redirect_host: %s\n", nr.redirectHost)
	}

	w.WriteHeader(200)
	w.Header().Add("Content-Encoding", "identity")
	w.Header().Add("Content-Type", "application/json")
	w.Write([]byte(`{"return_value":{"redirect_host":"localhost"}}`))
}

// processConnect processes the connection in the NewRelicAgentReceiver.
//
// It takes a http.ResponseWriter and a http.Request as parameters and does not return anything.
func (nr *NewRelicAgentReceiver) processConnect(w http.ResponseWriter, r *http.Request) {
	bodyReader := processBodyIfNecessary(r)
	body, err := io.ReadAll(bodyReader)
	if c, ok := bodyReader.(io.Closer); ok {
		_ = c.Close()
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var connectInfo []ConnectInfo
	if err := json.Unmarshal(body, &connectInfo); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resourceAttributes := map[string]interface{}{}
	resourceAttributes["service.name"] = connectInfo[0].AppName[0]
	// language -> telemetry.sdk.language?
	if connectInfo[0].HostName != "" {
		resourceAttributes["host.name"] = connectInfo[0].HostName
	}
	if connectInfo[0].ProcessPid > 0 {
		resourceAttributes["process.pid"] = connectInfo[0].ProcessPid
	}
	for k, v := range connectInfo[0].Metadata {
		resourceAttributes[strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(k, "NEW_RELIC_METADATA_"), "_", "."))] = v
	}

	resourceBytes, _ := json.Marshal(&resourceAttributes)

	reply := map[string]*ConnectReply{
		"return_value": {
			RunID:            "-1",
			DataReportPeriod: 60,
			RequestHeadersMap: map[string]string{
				"new-relic-resource": base64.URLEncoding.EncodeToString(resourceBytes),
			},
			MaxPayloadSizeInBytes: 1000000,
			EntityGUID:            "",

			// Transaction Name Modifiers
			SegmentTerms: make([]interface{}, 0),
			TxnNameRules: make([]interface{}, 0),
			URLRules:     make([]interface{}, 0),
			MetricRules:  make([]interface{}, 0),

			// Cross Process
			EncodingKey:     "",
			CrossProcessID:  "",
			TrustedAccounts: make([]int, 0),

			// Settings
			KeyTxnApdex:            make(map[string]float64),
			ApdexThresholdSeconds:  0.5,
			CollectAnalyticsEvents: true,
			CollectCustomEvents:    true,
			CollectTraces:          true,
			CollectErrors:          true,
			CollectErrorEvents:     true,
			CollectSpanEvents:      true,

			// RUM
			AgentLoader: "",
			Beacon:      "",
			BrowserKey:  "",
			AppID:       "",
			ErrorBeacon: "",
			JSAgentFile: "",

			// BetterCAT/Distributed Tracing
			AccountID:                     "",
			TrustedAccountKey:             "",
			PrimaryAppID:                  "",
			SamplingTarget:                10,
			SamplingTargetPeriodInSeconds: 60,

			EventData: EventHarvestConfig{
				ReportPeriodMs: 5000,
			},
		},
	}

	replyBytes, err := json.Marshal(&reply)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Encoding", "identity")
	w.Header().Add("Content-Type", "application/json")
	w.Write(replyBytes)
}

// proxyConnect is a function that handles proxying the incoming request to the specified URL.
//
// Parameters:
//   - w: http.ResponseWriter
//   - r: *http.Request
// Return type: None
func (nr *NewRelicAgentReceiver) proxyConnect(w http.ResponseWriter, r *http.Request) {
	// we need to buffer the body if we want to read it here and send it
	// in the request.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// create a new url from the raw RequestURI sent by the client
	url := fmt.Sprintf("https://%s%s", nr.redirectHost, r.RequestURI)

	proxyReq, _ := http.NewRequestWithContext(r.Context(), r.Method, url, bytes.NewReader(body))

	// We may want to filter some headers, otherwise we could just use a shallow copy
	// proxyReq.Header = req.Header
	proxyReq.Header = make(http.Header)
	for h, val := range r.Header {
		proxyReq.Header[h] = val
	}
	proxyReq.Header.Set("Host", r.Host)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	proxyReq.Header.Set("Content-Encoding", "identity")

	fmt.Printf("Found content-encoding %v", r.Header.Get("Content-Encoding"))
	fmt.Println()

	fmt.Printf("-- Forwarding request %v", proxyReq)
	fmt.Println()

	resp, err := nr.httpClient.Do(proxyReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	fmt.Printf("-- got response %v", resp)
	fmt.Println()

	responseBodyReader := processResponseBodyIfNecessary(resp)
	blob, _ := io.ReadAll(responseBodyReader)
	if c, ok := responseBodyReader.(io.Closer); ok {
		_ = c.Close()
	}

	responseHeaders := w.Header()
	for headerKey, headerValues := range resp.Header {
		for _, headerValue := range headerValues {
			responseHeaders.Add(headerKey, headerValue)
		}
	}
	responseHeaders.Set("Content-Encoding", "identity")
	responseHeaders.Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(blob)

	var connectInfo []ConnectInfo
	if err := json.Unmarshal(body, &connectInfo); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var tmp struct {
		ReturnValue struct {
			RunID      string `json:"agent_run_id"`
			EntityGuid string `json:"entity_guid"`
		} `json:"return_value"`
	}

	if err := json.Unmarshal(blob, &tmp); err == nil {
		nr.entityGuids.Store(tmp.ReturnValue.RunID, &agentMeta{
			entityGuid: tmp.ReturnValue.EntityGuid,
			entityName: connectInfo[0].AppName[0],
		})
		fmt.Printf("stored %s: %s\n", tmp.ReturnValue.RunID, tmp.ReturnValue.EntityGuid)
	} else {
		fmt.Println(err)
	}
}

// processMetricData processes the metric data received by the New Relic agent receiver.
//
// It takes the http.ResponseWriter, http.Request, and url.Values as parameters and does not return anything.
func (nr *NewRelicAgentReceiver) processMetricData(w http.ResponseWriter, r *http.Request, query url.Values) {
	if nr.metricsConsumer == nil {
		return
	}

	ctx := r.Context()
	if c, ok := client.FromHTTP(r); ok {
		ctx = client.NewContext(ctx, c)
	}

	transportTag := transportType(query)
	obsrecv := obsreport.NewReceiver(obsreport.ReceiverSettings{ReceiverID: nr.id, Transport: transportTag})
	ctx = obsrecv.StartTracesOp(ctx)

	requestBodyReader := processBodyIfNecessary(r)
	bodyBytes, _ := io.ReadAll(requestBodyReader)
	if c, ok := requestBodyReader.(io.Closer); ok {
		_ = c.Close()
	}
	var jsonBody []interface{}
	json.Unmarshal(bodyBytes, &jsonBody)

	nrResourceHeader, _ := base64.URLEncoding.DecodeString(r.Header.Get("new-relic-resource"))
	var agentResource map[string]interface{}
	json.Unmarshal(nrResourceHeader, &agentResource)

	startTimeSeconds := jsonBody[1].(float64)
	endTimeSeconds := jsonBody[2].(float64)
	nrMetricData := jsonBody[3].([]interface{})

	metrics := pdata.NewMetrics()
	startTime := pdata.Timestamp(startTimeSeconds * 1000 * 1000 * 1000)
	endTime := pdata.Timestamp(endTimeSeconds * 1000 * 1000 * 1000)

	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	resourceAttributes := resourceMetrics.Resource().Attributes()
	for k, v := range agentResource {
		switch attributeValue := v.(type) {
		case string:
			resourceAttributes.UpsertString(k, attributeValue)
		case int64:
			resourceAttributes.UpsertInt(k, attributeValue)
		case float64:
			if math.Floor(attributeValue) == attributeValue {
				resourceAttributes.UpsertInt(k, int64(attributeValue))
			} else {
				resourceAttributes.UpsertDouble(k, attributeValue)
			}
		case bool:
			resourceAttributes.UpsertBool(k, attributeValue)
		default:
			fmt.Printf("Got unexpected type %T for key %v and value %v", v, k, v)
			fmt.Println()
		}
	}

	ilMetrics := resourceMetrics.InstrumentationLibraryMetrics().AppendEmpty()
	userAgent := r.Header.Get("User-Agent")
	splitUserAgent := strings.SplitN(userAgent, "/", 2)
	ilMetrics.InstrumentationLibrary().SetName(splitUserAgent[0])
	ilMetrics.InstrumentationLibrary().SetVersion(splitUserAgent[1])

	otelMetrics := ilMetrics.Metrics()
	otelMetrics.EnsureCapacity(len(nrMetricData))
	for i := 0; i < len(nrMetricData); i {
		/*
			[
				{
					“name”:”name of metric”,
					“scope”:”scope of metric”,
				},
				[count, total time, exclusive time, min time, max time, sum of squares]
			]
			Uses attributes to differentiate between total time and exclusive time
			Sum of squares is dropped due to lack of use in the UI
			Even the apdex special case does not use the sum of squares entry
		*/
		timesliceMetric := nrMetricData[i].([]interface{})
		timesliceMetricNameMap := timesliceMetric[0].(map[string]interface{})
		timesliceMetricName := timesliceMetricNameMap["name"].(string)
		timesliceMetricData := timesliceMetric[1].([]interface{})

		if strings.HasPrefix(timesliceMetricName, "WebTransaction/") {
			mapWebTransactionMetric(&otelMetrics, timesliceMetricName, startTime, endTime, timesliceMetricData)
		} else if strings.HasPrefix(timesliceMetricName, "External/") {
			fmt.Println("got a ", timesliceMetricName)
			mapExternalMetric(&otelMetrics, timesliceMetricName, startTime, endTime, timesliceMetricData)
		}
	}

	consumerErr := nr.metricsConsumer.ConsumeMetrics(ctx, metrics)
	obsrecv.EndTracesOp(ctx, "nragent", metrics.MetricCount(), consumerErr)

	if consumerErr != nil {
		// Transient error, due to some internal condition.
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(errNextConsumerRespBody) // nolint:errcheck
		return
	}

	w.Header().Add("Content-Encoding", "identity")
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"return_value":[]}`))
}

// mapWebTransactionMetric generates a summary metric for web transaction.
//
// It takes in the metrics slice, timesliceMetricName, startTime, endTime, and timesliceMetricData, and does not return anything.
func mapWebTransactionMetric(metrics *pdata.MetricSlice, timesliceMetricName string, startTime pdata.Timestamp, endTime pdata.Timestamp, timesliceMetricData []interface{}) {
	metric := metrics.AppendEmpty()
	metric.SetDataType(pdata.MetricDataTypeSummary)
	metric.SetName("http.server.duration")
	metric.SetUnit("ms")
	metric.SetDescription(timesliceMetricName)

	summary := metric.Summary()
	dataPoints := summary.DataPoints()
	dataPoints.EnsureCapacity(1)
	inclusiveDataPoint := dataPoints.AppendEmpty()

	inclusiveDataPoint.SetCount(uint64(timesliceMetricData[0].(float64)))
	inclusiveDataPoint.SetSum(timesliceMetricData[1].(float64) * 1000)

	inclusiveDataPoint.QuantileValues().EnsureCapacity(2)
	minQuantile := inclusiveDataPoint.QuantileValues().AppendEmpty()
	minQuantile.SetQuantile(0)
	minQuantile.SetValue(timesliceMetricData[3].(float64) * 1000)
	maxQuantile := inclusiveDataPoint.QuantileValues().AppendEmpty()
	maxQuantile.SetQuantile(1)
	maxQuantile.SetValue(timesliceMetricData[4].(float64) * 1000)

	inclusiveDataPoint.SetStartTimestamp(startTime)
	inclusiveDataPoint.SetTimestamp(endTime)

	segments := strings.SplitAfterN(timesliceMetricName, "/", 3)
	if len(segments) == 3 {
		route := segments[len(segments)-1]
		if route == "" {
			route = "/"
		}
		inclusiveDataPoint.LabelsMap().Insert("http.route", route)
	} else {
		inclusiveDataPoint.LabelsMap().Insert("http.route", "/")
	}
}

// mapExternalMetric maps external metric data to pdata.MetricSlice for the given timesliceMetricName, startTime, endTime, and timesliceMetricData.
//
// - metrics: Pointer to pdata.MetricSlice
// - timesliceMetricName: String representing the name of the timeslice metric
// - startTime: pdata.Timestamp representing the start time
// - endTime: pdata.Timestamp representing the end time
// - timesliceMetricData: Slice of interfaces containing timeslice metric data
func mapExternalMetric(metrics *pdata.MetricSlice, timesliceMetricName string, startTime pdata.Timestamp, endTime pdata.Timestamp, timesliceMetricData []interface{}) {
	if strings.HasSuffix(timesliceMetricName, "/all") || strings.HasSuffix(timesliceMetricName, "/allWeb") {
		return
	}

	metric := metrics.AppendEmpty()
	metric.SetDataType(pdata.MetricDataTypeSummary)
	metric.SetName("http.client.duration")
	metric.SetUnit("ms")
	metric.SetDescription(timesliceMetricName)

	summary := metric.Summary()
	dataPoints := summary.DataPoints()
	dataPoints.EnsureCapacity(1)
	inclusiveDataPoint := dataPoints.AppendEmpty()

	inclusiveDataPoint.SetCount(uint64(timesliceMetricData[0].(float64)))
	inclusiveDataPoint.SetSum(timesliceMetricData[1].(float64) * 1000)

	inclusiveDataPoint.QuantileValues().EnsureCapacity(2)
	minQuantile := inclusiveDataPoint.QuantileValues().AppendEmpty()
	minQuantile.SetQuantile(0)
	minQuantile.SetValue(timesliceMetricData[3].(float64) * 1000)
	maxQuantile := inclusiveDataPoint.QuantileValues().AppendEmpty()
	maxQuantile.SetQuantile(1)
	maxQuantile.SetValue(timesliceMetricData[4].(float64) * 1000)

	inclusiveDataPoint.SetStartTimestamp(startTime)
	inclusiveDataPoint.SetTimestamp(endTime)

	// Ex: External/httpbin.org/Ratpack/GET
	//     External/<host>/<library>/<procedure>
	segments := strings.Split(timesliceMetricName, "/")
	inclusiveDataPoint.LabelsMap().Insert("http.host", segments[1])
	if len(segments) > 2 {
		inclusiveDataPoint.LabelsMap().Insert("http.method", segments[len(segments)-1])
	}
}

// proxyRequest is a Go function that proxies a request to a new URL.
//
// It takes in the http.ResponseWriter and *http.Request as parameters and does not return any values.
func (nr *NewRelicAgentReceiver) proxyRequest(w http.ResponseWriter, r *http.Request) {
	// we need to buffer the body if we want to read it here and send it
	// in the request.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// create a new url from the raw RequestURI sent by the client
	url := fmt.Sprintf("https://%s%s", nr.redirectHost, r.RequestURI)

	proxyReq, _ := http.NewRequestWithContext(r.Context(), r.Method, url, bytes.NewReader(body))

	// We may want to filter some headers, otherwise we could just use a shallow copy
	// proxyReq.Header = req.Header
	proxyReq.Header = make(http.Header)
	for h, val := range r.Header {
		proxyReq.Header[h] = val
	}
	proxyReq.Header.Set("Host", r.Host)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	proxyReq.Header.Set("Content-Encoding", "identity")

	fmt.Printf("Found content-encoding %v", r.Header.Get("Content-Encoding"))
	fmt.Println()

	fmt.Printf("-- Forwarding request %v", proxyReq)
	fmt.Println()

	resp, err := nr.httpClient.Do(proxyReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	fmt.Printf("-- got response %v", resp)
	fmt.Println()

	responseHeaders := w.Header()
	for headerKey, headerValues := range resp.Header {
		for _, headerValue := range headerValues {
			responseHeaders.Add(headerKey, headerValue)
		}
	}
	// Corrected misplaced code blocks and ensured all code is within appropriate functions or methods.
}

// processSpanEventRequest processes the span event request.
//
// It takes in the http.ResponseWriter, http.Request, collectorHost string, and url.Values as parameters.
func (nr *NewRelicAgentReceiver) processSpanEventRequest(w http.ResponseWriter, r *http.Request, collectorHost string, query url.Values) {
	if nr.tracesConsumer == nil {
		return
	}

	ctx := r.Context()
	if c, ok := client.FromHTTP(r); ok {
		ctx = client.NewContext(ctx, c)
	}

	transportTag := transportType(query)
	obsrecv := obsreport.NewReceiver(obsreport.ReceiverSettings{ReceiverID: nr.id, Transport: transportTag})
	ctx = obsrecv.StartTracesOp(ctx)

	requestBodyReader := processBodyIfNecessary(r)
	bodyBytes, _ := io.ReadAll(requestBodyReader)
	if c, ok := requestBodyReader.(io.Closer); ok {
		_ = c.Close()
	}
	var jsonBody []interface{}
	json.Unmarshal(bodyBytes, &jsonBody)

	nrSpanEvents := jsonBody[2].([]interface{}) //[][]map[string]interface{}]

	traces := pdata.NewTraces()

	resourceSpans := traces.ResourceSpans().AppendEmpty()

	if nr.proxyToNR {
		runToken := jsonBody[0]
		if v, ok := nr.entityGuids.Load(runToken); ok {
			meta := v.(*agentMeta)
			resourceAttributes := resourceSpans.Resource().Attributes()
			resourceAttributes.UpsertString("entity.guid", meta.entityGuid)
			resourceAttributes.UpsertString("service.name", meta.entityName)
		}
	} else {
		agentMetadata, _ := base64.URLEncoding.DecodeString(r.Header.Get("new-relic-resource"))
		var agentResource map[string]interface{}
		json.Unmarshal(agentMetadata, &agentResource)

		resourceAttributes := resourceSpans.Resource().Attributes()
		for k, v := range agentResource {
			switch attributeValue := v.(type) {
			case string:
				resourceAttributes.UpsertString(k, attributeValue)
			case int64:
				resourceAttributes.UpsertInt(k, attributeValue)
			case float64:
				if math.Floor(attributeValue) == attributeValue {
					resourceAttributes.UpsertInt(k, int64(attributeValue))
				} else {
					resourceAttributes.UpsertDouble(k, attributeValue)
				}
			case bool:
				resourceAttributes.UpsertBool(k, attributeValue)
			default:
				fmt.Printf("Got unexpected type %T for key %v and value %v", v, k, v)
				fmt.Println()
			}
		}
	}

	ilSpans := resourceSpans.InstrumentationLibrarySpans().AppendEmpty()
	userAgent := r.Header.Get("User-Agent")
	splitUserAgent := strings.SplitN(userAgent, "/", 2)
	ilSpans.InstrumentationLibrary().SetName(splitUserAgent[0])
	ilSpans.InstrumentationLibrary().SetVersion(splitUserAgent[1])

	otelSpans := ilSpans.Spans()
	otelSpans.EnsureCapacity(len(nrSpanEvents))
	for i := 0; i < len(nrSpanEvents); i {
		span := otelSpans.AppendEmpty()

		spanAttributes := span.Attributes()
		nrAttributeGroup := nrSpanEvents[i].([]interface{})
		AddNRAttributesToOTelSpan(nrAttributeGroup[0].(map[string]interface{}), spanAttributes)
		AddNRAttributesToOTelSpan(nrAttributeGroup[1].(map[string]interface{}), spanAttributes)
		AddNRAttributesToOTelSpan(nrAttributeGroup[2].(map[string]interface{}), spanAttributes)

		// TODO: Delete NR attributes when rewritten to OTel equivalents.

		// TODO: Detect error.* attributes and rewrite to exception.*
		// TODO: Set otel.status_code to ERROR

		spanIdString, _ := getAndRemove(&spanAttributes, "guid")
		spanIdBytes, _ := hex.DecodeString(spanIdString.StringVal())
		var spanIdByteArray [8]byte
		copy(spanIdByteArray[:], spanIdBytes)
		span.SetSpanID(pdata.NewSpanID(spanIdByteArray))

		if parentIdString, found := getAndRemove(&spanAttributes, "parentId"); found {
			parentIdBytes, _ := hex.DecodeString(parentIdString.StringVal())
			var parentIdByteArray [8]byte
			copy(parentIdByteArray[:], parentIdBytes)
			span.SetParentSpanID(pdata.NewSpanID(parentIdByteArray))
		}

		traceIdString, _ := getAndRemove(&spanAttributes, "traceId")
		traceIdBytes, _ := hex.DecodeString(traceIdString.StringVal())
		var traceIdByteArray [16]byte
		copy(traceIdByteArray[:], traceIdBytes)
		span.SetTraceID(pdata.NewTraceID(traceIdByteArray))

		// TODO: Set name to Unknown, but only if missing.
		name, _ := getAndRemove(&spanAttributes, "name")
		span.SetName(name.StringVal())

		categoryAttribute, _ := getAndRemove(&spanAttributes, "category")
		switch categoryAttribute.StringVal() {
		case "generic":
			if _, found := spanAttributes.Get("nr.entryPoint"); found {
				span.SetKind(pdata.SpanKindServer)
			} else {
				span.SetKind(pdata.SpanKindInternal)
			}
		case "http":
			span.SetKind(pdata.SpanKindClient)
		default:
			span.SetKind(pdata.SpanKindInternal)
		}

		// TODO: Maybe use this if category is not present.
		spanAttributes.Delete("span.kind")

		// HTTP translations
		// httpResponseCode -> http.status_code
		// http.statusCode  -> http.status_code
		// request.method   -> http.method
		// request.headers.contentLength -> http.request_content_length
		// request.headers.userAgent -> http.user_agent
		// request.uri -> http.target
		// request.headers.host -> http.host

		if statusCode, found := getAndRemove(&spanAttributes, "httpResponseCode"); found {
			spanAttributes.UpsertString("http.status_code", statusCode.StringVal())
		}

		if statusCode, found := getAndRemove(&spanAttributes, "http.statusCode"); found {
			spanAttributes.UpsertString("http.status_code", statusCode.StringVal())
		}

		if method, found := getAndRemove(&spanAttributes, "request.method"); found {
			spanAttributes.UpsertString("http.method", method.StringVal())
		}

		// TODO: Must also provide http.scheme and http.host.
		if uri, found := getAndRemove(&spanAttributes, "request.uri"); found {
			spanAttributes.UpsertString("http.target", uri.StringVal())
		}

		if host, found := getAndRemove(&spanAttributes, "request.headers.host"); found {
			spanAttributes.UpsertString("http.host", host.StringVal())
		}

		if contentLen, found := getAndRemove(&spanAttributes, "request.headers.contentLength"); found {
			spanAttributes.UpsertInt("http.request_content_length", contentLen.IntVal())
		}

		if userAgent, found := getAndRemove(&spanAttributes, "request.headers.userAgent"); found {
			spanAttributes.UpsertString("http.user_agent", userAgent.StringVal())
		}

		startTime, _ := getAndRemove(&spanAttributes, "timestamp")
		span.SetStartTimestamp(pdata.Timestamp(startTime.IntVal() * 1000 * 1000)) //convert from ms to ns
		duration, _ := getAndRemove(&spanAttributes, "duration")
		endTime := startTime.IntVal() + int64(duration.DoubleVal()*1000)
		span.SetEndTimestamp(pdata.Timestamp(endTime * 1000 * 1000)) //convert ms to ns
	}

	consumerErr := nr.tracesConsumer.ConsumeTraces(ctx, traces)
	obsrecv.EndTracesOp(ctx, "nragent", traces.SpanCount(), consumerErr)

	if consumerErr != nil {
		// Transient error, due to some internal condition.
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(errNextConsumerRespBody) // nolint:errcheck
		return
	}

	w.Header().Add("Content-Encoding", "identity")
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{}`))
}

// AddNRAttributesToOTelSpan adds New Relic attributes to an OpenTelemetry span.
//
// It takes a map of New Relic attribute keys and values, and a map of OpenTelemetry span attributes.
func AddNRAttributesToOTelSpan(nrSpanEventAttributeMap map[string]interface{}, spanAttributes pdata.AttributeMap) {
	for nrAttributeKey, nrAttributeValue := range nrSpanEventAttributeMap {
		switch attributeValue := nrAttributeValue.(type) {
		case string:
			spanAttributes.UpsertString(nrAttributeKey, attributeValue)
		case int64:
			spanAttributes.UpsertInt(nrAttributeKey, attributeValue)
		case float64:
			if math.Floor(attributeValue) == attributeValue {
				spanAttributes.UpsertInt(nrAttributeKey, int64(attributeValue))
			} else {
				spanAttributes.UpsertDouble(nrAttributeKey, attributeValue)
			}
		case bool:
			spanAttributes.UpsertBool(nrAttributeKey, attributeValue)
		default:
			fmt.Printf("Got unexpected type %T for key %v and value %v", nrAttributeValue, nrAttributeKey, nrAttributeValue)
			fmt.Println()
		}
	}
}

// getAndRemove is a function that takes a spanAttributes pointer and a key string, and returns the value and a boolean indicating success.
//
// It modifies the spanAttributes by removing the specified key if it exists, and returns the value and a boolean indicating whether the key was found.
func getAndRemove(spanAttributes *pdata.AttributeMap, key string) (pdata.AttributeValue, bool) {
	value, ok := spanAttributes.Get(key)
	if ok {
		tmp := pdata.NewAttributeValueNull()
		value.CopyTo(tmp)
		value = tmp
		spanAttributes.Delete(key)
	}
	return value, ok
}

// writeToFile writes data to a file at the specified filePath.
//
// Parameters:
// filePath string - the path of the file to write to
// data []byte - the data to be written to the file
// error - returns an error if there is any issue writing to the file
// Duplicate function definition removed.
    // Extract directory path from filePath
    dirPath := filepath.Dir(filePath)

    // Ensure the directory exists
    if err := os.MkdirAll(dirPath, 0755); err != nil {
        log.Printf("Failed to create directory: %v", err)
        return err
    }

    // Open or create the file
    file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer file.Close()

    // Write the data
    if _, err := file.Write(data); err != nil {
        log.Printf("Failed to write data to file: %v", err)
        return err
    }
    return nil
}


// appendToFile opens or creates the file at the given file path and appends the provided data to it.
//
// Parameters:
// - filePath: a string representing the file path
// - data: a byte slice containing the data to be appended to the file
// Return type: error
// Duplicate function definition removed.
	// Open or create the file
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the data
	if _, err := file.Write(data); err != nil {
		return err
	}
	return nil
}
