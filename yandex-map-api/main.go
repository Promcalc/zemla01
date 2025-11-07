package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

type LotPoint struct {
	Lat            float64 `json:"lat"`
	Lon            float64 `json:"lon"`
	LotName        string  `json:"lotName"`
	LotDescription string  `json:"lotDescription"`
	Link           string  `json:"link"`
}

type LotInfo struct {
	Point struct {
		Lat float64 `json:"lat"`
		Lon float64 `json:"lon"`
	} `json:"point"`
	LotName        string `json:"lotName"`
	LotDescription string `json:"lotDescription"`
}

// normalizeHeader — приводит заголовок к каноничному виду (регистронезависимо, пробелы)
func normalizeHeader(s string) string {
	return strings.TrimSpace(strings.ToLower(s))
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("⚠️ .env не найден, используем переменные из окружения")
	}

	sheetID := os.Getenv("GOOGLE_SHEET_ID")
	credentialsJSON := os.Getenv("GOOGLE_CREDENTIALS")
	sheetName := os.Getenv("SHEET_NAME")
	if sheetName == "" {
		sheetName = "Sheet1"
	}

	if sheetID == "" || credentialsJSON == "" {
		log.Fatal("❌ Требуются GOOGLE_SHEET_ID и GOOGLE_CREDENTIALS в .env")
	}

	ctx := context.Background()
	sheetsService, err := sheets.NewService(ctx, option.WithCredentialsJSON([]byte(credentialsJSON)))
	if err != nil {
		log.Fatalf("❌ Ошибка создания Google Sheets клиента: %v", err)
	}

	http.HandleFunc("/api/points", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.Header().Set("Content-Type", "application/json")

		if r.Method != http.MethodGet {
			http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
			return
		}

		// 1. Читаем первую строку — заголовки
		headerRange := sheetName + "!1:1"
		headerResp, err := sheetsService.Spreadsheets.Values.Get(sheetID, headerRange).Do()
		if err != nil {
			log.Printf("❌ Ошибка чтения заголовков: %v", err)
			http.Error(w, "Ошибка чтения структуры таблицы", http.StatusInternalServerError)
			return
		}

		var headers []string
		if len(headerResp.Values) > 0 {
			for _, cell := range headerResp.Values[0] {
				if str, ok := cell.(string); ok {
					headers = append(headers, str)
				} else {
					headers = append(headers, "")
				}
			}
		}

		// 2. Ищем индексы нужных колонок
		var lotInfoIndex, linkIndex int = -1, -1
		for i, h := range headers {
			norm := normalizeHeader(h)
			if norm == "lot_info" || norm == "lot info" {
				lotInfoIndex = i
			}
			if norm == "link" {
				linkIndex = i
			}
		}

		if lotInfoIndex == -1 {
			log.Println("❌ Колонка 'Lot_info' не найдена в заголовках")
			http.Error(w, "Колонка 'Lot_info' не найдена", http.StatusBadRequest)
			return
		}
		if linkIndex == -1 {
			log.Println("❌ Колонка 'Link' не найдена в заголовках")
			http.Error(w, "Колонка 'Link' не найдена", http.StatusBadRequest)
			return
		}

		// 3. Читаем все данные (начиная со 2-й строки)
		dataRange := sheetName + "!2:10000" // можно увеличить при необходимости
		dataResp, err := sheetsService.Spreadsheets.Values.Get(sheetID, dataRange).Do()
		if err != nil {
			log.Printf("❌ Ошибка чтения данных: %v", err)
			http.Error(w, "Ошибка чтения данных", http.StatusInternalServerError)
			return
		}

		var points []LotPoint

		for rowIndex, row := range dataResp.Values {
			// Пропускаем пустые строки
			if len(row) == 0 {
				continue
			}

			// Получаем значение Lot_info
			var lotInfoStr string
			if lotInfoIndex < len(row) {
				if s, ok := row[lotInfoIndex].(string); ok {
					lotInfoStr = s
				}
			}
			if lotInfoStr == "" {
				continue // пропускаем, если нет данных
			}

			// Получаем значение Link
			var linkStr string
			if linkIndex < len(row) {
				if s, ok := row[linkIndex].(string); ok {
					linkStr = s
				}
			}

			// Парсим JSON
			var lot LotInfo
			if err := json.Unmarshal([]byte(lotInfoStr), &lot); err != nil {
				log.Printf("⚠️ Ошибка парсинга Lot_info в строке %d: %v", rowIndex+2, err)
				continue
			}

			// Пропускаем, если нет координат
			if lot.Point.Lat == 0 && lot.Point.Lon == 0 {
				continue
			}

			points = append(points, LotPoint{
				Lat:            lot.Point.Lat,
				Lon:            lot.Point.Lon,
				LotName:        lot.LotName,
				LotDescription: lot.LotDescription,
				Link:           linkStr,
			})
		}

		log.Printf("✅ Найдено %d точек для отображения", len(points))
		if err := json.NewEncoder(w).Encode(points); err != nil {
			log.Printf("❌ Ошибка отправки JSON: %v", err)
			http.Error(w, "Ошибка сериализации", http.StatusInternalServerError)
		}
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("✅ Сервер запущен на порту %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}