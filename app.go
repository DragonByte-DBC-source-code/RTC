package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type Peer struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Client struct {
	Peer
	Room string
	conn *websocket.Conn
	mu   sync.Mutex
}

type Message struct {
	Type   string `json:"type"`
	PeerID string `json:"peerId,omitempty"` // Used for join
	Name   string `json:"name,omitempty"`   // Used for join
	Room   string `json:"room,omitempty"`   // Used for join
	From   string `json:"from,omitempty"`   // Source of signal/leave
	To     string `json:"to,omitempty"`     // Target of signal
	Data   any    `json:"data,omitempty"`   // SDP or ICE candidate
}

type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client
}

func NewHub() *Hub {
	return &Hub{clients: make(map[string]*Client)}
}

func (h *Hub) register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c.ID] = c
}

func (h *Hub) unregister(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, id)
}

func (h *Hub) peersInRoom(room, excludeID string) []*Client {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var peers []*Client
	for _, cl := range h.clients {
		if cl.Room == room && cl.ID != excludeID {
			peers = append(peers, cl)
		}
	}
	return peers
}

func (h *Hub) get(id string) *Client {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.clients[id]
}

func send(c *Client, msg Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.conn.WriteJSON(msg); err != nil {
		log.Printf("error sending to %s: %v", c.ID, err)
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func serveWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	cl := &Client{conn: conn}

	defer func() {
		conn.Close()
		if cl.ID != "" {
			hub.unregister(cl.ID)
			// Notify peers
			peers := hub.peersInRoom(cl.Room, cl.ID)
			for _, p := range peers {
				send(p, Message{Type: "peer-left", From: cl.ID})
			}
		}
	}()

	for {
		var msg Message
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}

		switch msg.Type {
		case "join":
			cl.ID = msg.PeerID
			cl.Name = msg.Name
			cl.Room = msg.Room
			hub.register(cl)

			existing := hub.peersInRoom(cl.Room, cl.ID)
			
			// 1. Send existing peer list to the newcomer
			peerList := make([]Peer, 0)
			for _, p := range existing {
				peerList = append(peerList, p.Peer)
			}
			send(cl, Message{Type: "existing-peers", Data: peerList})

			// 2. Notify existing peers that someone new joined
			for _, p := range existing {
				send(p, Message{
					Type: "peer-joined", 
					From: cl.ID, 
					Name: cl.Name,
				})
			}

		case "signal":
			if msg.To == "" {
				continue
			}
			target := hub.get(msg.To)
			if target != nil {
				// Forward the signal, ensuring 'From' is set to the sender's ID
				msg.From = cl.ID
				send(target, msg)
			}
		}
	}
}

func main() {
	hub := NewHub()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serveWS(hub, w, r)
	})
	log.Println("Server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}