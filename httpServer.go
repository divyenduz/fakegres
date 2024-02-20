package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/hashicorp/raft"
)

type httpServer struct {
	r *raft.Raft
}

func (hs httpServer) addFollowerHandler(w http.ResponseWriter, r *http.Request) {
	followerId := r.URL.Query().Get("id")
	followerAddr := r.URL.Query().Get("addr")

	if hs.r.State() != raft.Leader {
		json.NewEncoder(w).Encode(struct {
			Error string `json:"error"`
		}{
			"Not the leader",
		})
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	err := hs.r.AddVoter(raft.ServerID(followerId), raft.ServerAddress(followerAddr), 0, 0).Error()
	if err != nil {
		log.Printf("Failed to add follower: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}
