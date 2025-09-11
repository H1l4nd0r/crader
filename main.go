package main

import (
	"encoding/json"
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
	conn, _, err := websocket.DefaultDialer.Dial("wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/myxusdt@trade", nil)
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

		var response struct {
			Stream string `json:"stream"`
			Data   struct {
				Symbol string `json:"s"`
				Price  string `json:"p"`
			} `json:"data"`
		}
		if err := json.Unmarshal(message, &response); err != nil {
			log.Printf("Error unmarshalling message from Binance: %v", err)
			continue
		}

		price, err := strconv.ParseFloat(response.Data.Price, 64)
		if err != nil {
			log.Printf("Error parsing price from Binance: %v", err)
			continue
		}

		ex.mu.Lock()
		ex.PrevPrices[response.Data.Symbol] = ex.Prices[response.Data.Symbol]
		ex.Prices[response.Data.Symbol] = price
		ex.mu.Unlock()
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
	subMsg := `{"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT"}, {"channel": "trades", "instId": "ETH-USDT"}, {"channel": "trades", "instId": "MYX-USDT"}]}`

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

		var response struct {
			Event string `json:"event"`
			Arg   struct {
				Channel string `json:"channel"`
				InstId  string `json:"instId"`
			} `json:"arg"`
			Data []struct {
				InstId string `json:"instId"`
				Px     string `json:"px"`
			} `json:"data"`
		}
		if err := json.Unmarshal(message, &response); err != nil {
			log.Printf("Error unmarshalling message from OKX: %v", err)
			continue
		}

		if response.Event == "subscribe" {
			continue
		}

		for _, trade := range response.Data {
			price, err := strconv.ParseFloat(trade.Px, 64)
			if err != nil {
				log.Printf("Error parsing price from OKX: %v", err)
				continue
			}

			ex.mu.Lock()
			ex.PrevPrices[trade.InstId] = ex.Prices[trade.InstId]
			ex.Prices[trade.InstId] = price
			ex.mu.Unlock()
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
			"refRates":  ex.RefRates,
		}
		ex.mu.Unlock()
	}
	result["signals"] = generateSignals()
	return result
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
