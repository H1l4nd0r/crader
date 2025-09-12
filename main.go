package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Структуры данных
type ExchangeData struct {
	Connected    bool
	Prices       map[string]float64
	PrevPrices   map[string]float64
	RefRates     map[string]float64
	PrevRefRates map[string]float64
	mu           sync.Mutex
}

var exchanges = map[string]*ExchangeData{
	"binance": {Prices: make(map[string]float64), PrevPrices: make(map[string]float64), RefRates: make(map[string]float64), PrevRefRates: make(map[string]float64)},
	"bybit":   {Prices: make(map[string]float64), PrevPrices: make(map[string]float64), RefRates: make(map[string]float64), PrevRefRates: make(map[string]float64)},
	"okx":     {Prices: make(map[string]float64), PrevPrices: make(map[string]float64), RefRates: make(map[string]float64), PrevRefRates: make(map[string]float64)},
}

var wsClients = make(map[*websocket.Conn]bool)
var wsMu sync.Mutex

func main() {
	// Запуск симуляции биржевых данных
	go subscribeExchangeBinance()
	go subscribeExchangeBybit()
	go subscribeExchangeOKX()

	// HTTP сервер для фронтенда
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/ws", wsHandler)
	log.Println("Server started at :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

func subscribeExchangeBinance() {
	ex := exchanges["binance"]
	ex.Connected = true

	// Connect to Binance WebSocket
	conn, _, err := websocket.DefaultDialer.Dial("wss://fstream.binance.com/stream?streams=btcusdt@trade/ethusdt@trade/myxusdt@trade/btcusdt@markPrice/ethusdt@markPrice/myxusdt@markPrice", nil)
	if err != nil {
		log.Fatalf("Failed to connect to Binance WebSocket: %v", err)
	}
	defer conn.Close()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading message from Binance: %v", err)
			break
		}

		var stream struct {
			Stream string          `json:"stream"`
			Data   json.RawMessage `json:"data"`
		}

		// Для @trade
		var trade struct {
			Symbol string `json:"s"` // символ, например BTCUSDT
			Price  string `json:"p"` // цена сделки
		}

		// Для @fundingRate
		var funding struct {
			Event                string `json:"e"`
			EventTime            int64  `json:"E"`
			Symbol               string `json:"s"`
			MarkPrice            string `json:"p"`
			IndexPrice           string `json:"i"`
			EstimatedSettlePrice string `json:"P"`
			FundingRate          string `json:"r"`
			NextFundingTime      int64  `json:"T"`
		}

		if err := json.Unmarshal(message, &stream); err != nil {
			log.Printf("Error unmarshalling message from Binance: %v", err)
			continue
		}

		switch {
		case stream.Stream == "btcusdt@trade" || stream.Stream == "ethusdt@trade" || stream.Stream == "myxusdt@trade":

			if err := json.Unmarshal(stream.Data, &trade); err == nil {
				//fmt.Printf("TRADE %s price=%s\n", trade.Symbol, trade.Price)

				price, err := strconv.ParseFloat(trade.Price, 64)

				if err != nil {
					log.Printf("Error parsing price from Binance: %v", err)
					continue
				}

				ex.mu.Lock()
				ex.PrevPrices[trade.Symbol] = ex.Prices[trade.Symbol]
				ex.Prices[trade.Symbol] = price
				ex.mu.Unlock()
			}
		case stream.Stream == "btcusdt@markPrice" || stream.Stream == "ethusdt@markPrice" || stream.Stream == "myxusdt@markPrice":

			if err := json.Unmarshal(stream.Data, &funding); err == nil {
				//fmt.Printf("FUNDING %s rate=%s\n", funding.Symbol, funding.FundingRate)

				rate, err := strconv.ParseFloat(funding.FundingRate, 64)

				if err != nil {
					log.Printf("Error parsing price from Binance: %v", err)
					continue
				}

				ex.mu.Lock()
				ex.PrevRefRates[funding.Symbol] = ex.RefRates[funding.Symbol]
				ex.RefRates[funding.Symbol] = rate
				ex.mu.Unlock()
			}

		default:
			fmt.Println("UNKNOWN STREAM:", stream.Stream)
		}

	}
}

func subscribeExchangeBybit() {
	ex := exchanges["bybit"]
	ex.Connected = true

	// Connect to Bybit WebSocket
	conn, _, err := websocket.DefaultDialer.Dial("wss://stream.bybit.com/v5/public/linear", nil)
	if err != nil {
		log.Fatalf("Failed to connect to Bybit WebSocket: %v", err)
	}
	defer conn.Close()

	// Subscribe to BTCUSDT and ETHUSDT trades
	subMsg := `{"op": "subscribe", "args": ["tickers.BTCUSDT", "tickers.ETHUSDT", "tickers.MYXUSDT"]}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(subMsg)); err != nil {
		log.Fatalf("Failed to subscribe to Bybit WebSocket: %v", err)
	}

	// Wait for subscription confirmation
	_, _, err = conn.ReadMessage()
	if err != nil {
		log.Fatalf("Failed to read subscription confirmation from Bybit: %v", err)
	}

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading message from Bybit: %v", err)
			break
		}

		var msg struct {
			Type  string          `json:"type"`
			Topic string          `json:"topic"`
			Data  json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("Error unmarshalling message from Bybit: %v", err)
			continue
		}

		// Обработка snapshot
		if msg.Type == "snapshot" {
			var snapshotData struct {
				Ask1Price          string `json:"ask1Price"`
				Ask1Size           string `json:"ask1Size"`
				Bid1Price          string `json:"bid1Price"`
				Bid1Size           string `json:"bid1Size"`
				CurPreListingPhase string `json:"curPreListingPhase"`
				FundingRate        string `json:"fundingRate"`
				HighPrice24h       string `json:"highPrice24h"`
				IndexPrice         string `json:"indexPrice"`
				LastPrice          string `json:"lastPrice"`
				LowPrice24h        string `json:"lowPrice24h"`
				MarkPrice          string `json:"markPrice"`
				NextFundingTime    string `json:"nextFundingTime"`
				OpenInterest       string `json:"openInterest"`
				OpenInterestValue  string `json:"openInterestValue"`
				PreOpenPrice       string `json:"preOpenPrice"`
				PreQty             string `json:"preQty"`
				PrevPrice1h        string `json:"prevPrice1h"`
				PrevPrice24h       string `json:"prevPrice24h"`
				Price24hPcnt       string `json:"price24hPcnt"`
				Symbol             string `json:"symbol"`
				TickDirection      string `json:"tickDirection"`
				Turnover24h        string `json:"turnover24h"`
				Volume24h          string `json:"volume24h"`
			}
			if err := json.Unmarshal(msg.Data, &snapshotData); err != nil {
				log.Printf("Error unmarshalling snapshot data from Bybit: %v", err)
				continue
			}

			price, err := strconv.ParseFloat(snapshotData.LastPrice, 64)
			if err != nil {
				log.Printf("Error parsing price from Bybit: %v", err)
				continue
			}

			refRate, err := strconv.ParseFloat(snapshotData.FundingRate, 64)
			if err != nil {
				log.Printf("Error parsing refRate from Bybit: %v", err)
				continue
			}

			ex.mu.Lock()
			ex.PrevPrices[snapshotData.Symbol] = ex.Prices[snapshotData.Symbol]
			ex.Prices[snapshotData.Symbol] = price
			ex.PrevRefRates[snapshotData.Symbol] = ex.RefRates[snapshotData.Symbol]
			ex.RefRates[snapshotData.Symbol] = refRate
			ex.mu.Unlock()
		} else if msg.Type == "delta" {
			var deltaData struct {
				Symbol      string `json:"symbol"`
				LastPrice   string `json:"lastPrice"`
				FundingRate string `json:"fundingRate"`
			}
			if err := json.Unmarshal(msg.Data, &deltaData); err != nil {
				log.Printf("Error unmarshalling delta data from Bybit: %v", err)
				continue
			}

			if deltaData.LastPrice != "" || deltaData.FundingRate != "" {
				ex.mu.Lock()
				if deltaData.LastPrice != "" {
					price, err := strconv.ParseFloat(deltaData.LastPrice, 64)
					if err != nil {
						log.Printf("Error parsing price from Bybit: %v", err)
						ex.mu.Unlock()
						continue
					}
					ex.PrevPrices[deltaData.Symbol] = ex.Prices[deltaData.Symbol]
					ex.Prices[deltaData.Symbol] = price
				}
				if deltaData.FundingRate != "" {
					refRate, err := strconv.ParseFloat(deltaData.FundingRate, 64)
					if err != nil {
						log.Printf("Error parsing refRate from Bybit: %v", err)
						ex.mu.Unlock()
						continue
					}
					ex.PrevRefRates[deltaData.Symbol] = ex.RefRates[deltaData.Symbol]
					ex.RefRates[deltaData.Symbol] = refRate
				}
				ex.mu.Unlock()
			}
		}

	}
}

func subscribeExchangeOKX() {
	ex := exchanges["okx"]
	ex.Connected = true

	// Connect to OKX WebSocket
	conn, _, err := websocket.DefaultDialer.Dial("wss://ws.okx.com:8443/ws/v5/public", nil)
	if err != nil {
		log.Fatalf("Failed to connect to OKX WebSocket: %v", err)
	}
	defer conn.Close()

	// Subscribe to BTCUSDT and ETHUSDT trades
	subMsg := `{"op": "subscribe", "args": [
		{"channel": "tickers", "instId": "BTC-USDT-SWAP"}, 
		{"channel": "tickers", "instId": "ETH-USDT-SWAP"}, 
		{"channel": "tickers", "instId": "MYX-USDT-SWAP"}, 
		{"channel": "funding-rate","instId": "BTC-USDT-SWAP"}, 
		{"channel": "funding-rate","instId": "ETH-USDT-SWAP"}, 
		{"channel": "funding-rate","instId": "MYX-USDT-SWAP"}
	]}`

	if err := conn.WriteMessage(websocket.TextMessage, []byte(subMsg)); err != nil {
		log.Fatalf("Failed to subscribe to OKX WebSocket: %v", err)
	}

	// Wait for subscription confirmation
	_, _, err = conn.ReadMessage()
	if err != nil {
		log.Fatalf("Failed to read subscription confirmation from OKX: %v", err)
	}

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading message from OKX: %v", err)
			break
		}

		// Аргументы (канал, инструмент)
		type Arg struct {
			Channel string `json:"channel"`
			InstId  string `json:"instId"`
		}

		var resp struct {
			Arg  Arg             `json:"arg"`
			Data json.RawMessage `json:"data"`
		}

		// Структура для тикера
		var ticker []struct {
			InstType string `json:"instType"`
			InstId   string `json:"instId"`
			Last     string `json:"last"`
			AskPx    string `json:"askPx"`
			BidPx    string `json:"bidPx"`
			High24h  string `json:"high24h"`
			Low24h   string `json:"low24h"`
		}

		// Структура для фандинга
		var fr []struct {
			InstId          string `json:"instId"`
			FundingRate     string `json:"fundingRate"`
			NextFundingTime string `json:"nextFundingTime"`
		}

		if err := json.Unmarshal(message, &resp); err != nil {
			log.Printf("Error unmarshalling message from OKX: %v", err)
			continue
		}

		switch resp.Arg.Channel {
		case "tickers":

			if err := json.Unmarshal(resp.Data, &ticker); err == nil {
				//fmt.Printf("Цена %s: %s USDT (bid=%s ask=%s)\n",ticker[0].InstId, ticker[0].Last, ticker[0].BidPx, ticker[0].AskPx)

				price, err := strconv.ParseFloat(ticker[0].Last, 64)
				if err != nil {
					log.Printf("Error parsing price from OKX: %v", err)
					continue
				}

				ex.mu.Lock()
				ex.PrevPrices[ticker[0].InstId] = ex.Prices[ticker[0].InstId]
				ex.Prices[ticker[0].InstId] = price
				ex.mu.Unlock()
			}
		case "funding-rate":

			if err := json.Unmarshal(resp.Data, &fr); err == nil {
				//fmt.Printf("Funding %s: %s, next=%s\n", fr[0].InstId, fr[0].FundingRate, fr[0].NextFundingTime)

				refRate, err := strconv.ParseFloat(fr[0].FundingRate, 64)
				if err != nil {
					log.Printf("Error parsing refRate from Bybit: %v", err)
					continue
				}

				ex.mu.Lock()
				ex.PrevRefRates[fr[0].InstId] = ex.RefRates[fr[0].InstId]
				ex.RefRates[fr[0].InstId] = refRate
				ex.mu.Unlock()
			}
		default:
			fmt.Println("Неизвестный канал:", resp.Arg.Channel)
		}

	}
}

// Обработчик WebSocket для фронтенда
func wsHandler(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}

	wsMu.Lock()
	wsClients[conn] = true
	wsMu.Unlock()

	// Отправка данных каждые 500 мс
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		data := collectData()
		msg, _ := json.Marshal(data)
		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			log.Println("WriteMessage error:", err)
			break
		}
	}

	wsMu.Lock()
	delete(wsClients, conn)
	wsMu.Unlock()
	conn.Close()
}

// Сбор данных для фронтенда
func collectData() map[string]interface{} {
	result := make(map[string]interface{})
	for name, ex := range exchanges {
		ex.mu.Lock()
		result[name] = map[string]interface{}{
			"connected": ex.Connected,
			"prices":    ex.Prices,
			"refRates":  formatRefRates(ex.RefRates),
		}
		ex.mu.Unlock()
	}
	result["signals"] = generateSignals()
	return result
}

func formatRefRates(refRates map[string]float64) map[string]string {
	formattedRefRates := make(map[string]string)
	for symbol, rate := range refRates {
		formattedRefRates[symbol] = strconv.FormatFloat(rate, 'f', 8, 64)
	}
	return formattedRefRates
}

// Генерация сигналов если цена отличается >0.05%
func generateSignals() []map[string]interface{} {
	signals := []map[string]interface{}{}
	symbols := []string{"BTCUSDT", "ETHUSDT"}

	for _, sym := range symbols {
		prices := []struct {
			Name  string
			Price float64
			Rate  float64
		}{}
		for name, ex := range exchanges {
			ex.mu.Lock()
			p := ex.Prices[sym]
			r := ex.RefRates[sym]
			ex.mu.Unlock()
			prices = append(prices, struct {
				Name  string
				Price float64
				Rate  float64
			}{name, p, r})
		}

		for i := 0; i < len(prices); i++ {
			for j := i + 1; j < len(prices); j++ {
				if prices[i].Price > 0 && prices[j].Price > 0 {
					diff := abs(prices[i].Price-prices[j].Price) / ((prices[i].Price + prices[j].Price) / 2)
					if diff > 0.0005 { // 0.05%
						signals = append(signals, map[string]interface{}{
							"symbol": sym,
							"ex1":    prices[i].Name,
							"price1": prices[i].Price,
							"rate1":  prices[i].Rate,
							"ex2":    prices[j].Name,
							"price2": prices[j].Price,
							"rate2":  prices[j].Rate,
						})
					}
				}

			}
		}
	}

	return signals
}

func abs(a float64) float64 {
	if a < 0 {
		return -a
	}
	return a
}
