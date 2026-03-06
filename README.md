# Chordify

**Μάθημα:** Κατανεμημένα Συστήματα (9ο Εξάμηνο - Σχολή ΗΜΜΥ) 

**Ακαδημαϊκό Έτος:** 2025-2026 

**Ομάδα:** 12

---

## Περιγραφή Project
Υλοποίηση του **Chordify**, μιας P2P εφαρμογής ανταλλαγής τραγουδιών βασισμένη στο πρωτόκολλο **Chord DHT**. Το σύστημα υποστηρίζει δυναμική εισαγωγή/αποχώρηση κόμβων, κατακερματισμό κλειδιών (hashing) και replication δεδομένων με διαφορετικά μοντέλα συνέπειας.

---

## Usage
Ενεργοποίηση του bootstrap: cargo run --bin chordify <local_ip> -k <replication_factor> -t <consistency_type>
Είσοδος κόμβου: cargo run --bin chordify <local_ip> <bootstrap_ip>

## Dependecies
- gnome-terminal

## Progress Tracker / Roadmap

### Φάση 1: Επιλογές Αρχιτεκτονικής 
- [x] **Επιλογή Γλώσσας:** Rust (safe, fast, excellent async support)
- [x] **Επιλογή Βιβλιοθήκης:** Tokio (async runtime), Serde (serialization), SHA-1 (hashing), Anyhow (error handling), Tracing (logging)

### Φάση 2: Βασική Υποδομή Κόμβου (Node Infrastructure)
- [x] **Socket Setup:** Async TCP server/client με Tokio για concurrent request handling.
- [x] **ID Generation:** SHA-1 hash function στο `ip_address:port` για παραγωγή 160-bit unique ID.
- [x] **Message Protocol:** JSON-based custom protocol με Request/Response enums και length-prefixed framing.

### Φάση 3: Διαχείριση Μελών (Membership Management)
- [x] **Bootstrap Node:** Υλοποίηση σταθερού κόμβου για την αρχική σύνδεση.
- [x] **Node Join:** Υλοποίηση `join(nodeID)`. Ενημέρωση δεικτών και μεταφορά κλειδιών στον νέο κόμβο.
- [x] **Graceful Departure:** Υλοποίηση `depart(nodeID)`. Ενημέρωση γειτόνων και ανακατανομή κλειδιών πριν την έξοδο.

### Φάση 4: Λειτουργίες DHT & Routing (Basic Chord)
- [x] **Routing Logic:** Κάθε κόμβος κρατάει pointers για `successor` και `predecessor`.
- [x] **Insert Operation:** Υλοποίηση `insert(key, value)`. Αν το key υπάρχει, γίνεται `concat` (update).
- [x] **Query Operation:** Υλοποίηση `query(key)` για εντοπισμό value. Χειρισμός του wildcard `*` για επιστροφή όλων των κλειδιών.
- [x] **Delete Operation:** Υλοποίηση `delete(key)`.
- [x] **Hashing Logic:** Hashing του `key` (τίτλος τραγουδιού) για εύρεση υπεύθυνου κόμβου.
- [ ] *(Bonus)* **Finger Tables:** Υλοποίηση λογαριθμικής δρομολόγησης (προαιρετικό).

### Φάση 5: Replication & Consistency
- [x] **Replication Strategy:** Αποθήκευση ζευγών `<key, value>` στον υπεύθυνο κόμβο και στους $k-1$ επόμενους.
- [x] **Linearizability (Strong Consistency):**
  - [x] Υλοποίηση μηχανισμού (Chain Replication).
- [x] **Eventual Consistency:**
  - [x] Lazy propagation των writes στους replicas.
  - [x] Reads από οποιονδήποτε replica (πιθανότητα stale data).

### Φάση 6: Client (CLI)
- [x] **Help:** Εκτύπωση οδηγιών.
- [x] **Insert:** `insert <key> <value>`.
- [x] **Delete:** `delete <key>`.
- [x] **Query:** `query <key>` (και υποστήριξη `query *`).
- [x] **Depart:** `depart`.
- [x] **Overlay:** `overlay` (Εκτύπωση τοπολογίας δακτυλίου).

### Φάση 7: Graphics (GUI) (optional)
- [x] *(Bonus)* **GUI:** Υλοποίηση gui για την κλήση των commands όπως στο CLI

### Φάση 8: Πειράματα (AWS)
- [x] **Setup:** Στήσιμο σε 10 κόμβους στο AWS.
- [x] **Experiment 1 (Write Throughput):** Insert keys από αρχεία `insert_n.txt` με $k=1, 3, 5$ (Linear & Eventual).
- [x] **Experiment 2 (Read Throughput):** Query keys από αρχεία `query_n.txt` με τα παραπάνω setups.
- [x] **Experiment 3 (Consistency Check):** Εκτέλεση `requests.txt` και σύγκριση φρεσκάδας δεδομένων (Linear vs Eventual).

### Φάση 9: Αναφορά
- [ ] **Αναφορά:** Σύνθεση αναφοράς και σχολιασμός αποτελεσμάτων
