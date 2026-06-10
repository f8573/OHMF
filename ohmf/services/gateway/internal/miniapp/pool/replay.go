package pool

import "encoding/json"

func AppendReplay(log ReplayLog, record ShotReplayRecord) ReplayLog {
	log.Records = append(log.Records, record)
	return log
}

func ReplayLogJSON(log ReplayLog) ([]byte, error) {
	return json.Marshal(log)
}

func ParseReplayLog(data []byte) (ReplayLog, error) {
	var log ReplayLog
	err := json.Unmarshal(data, &log)
	return log, err
}
