# Chordify

**Μάθημα:** Κατανεμημένα Συστήματα (9ο Εξάμηνο - Σχολή ΗΜΜΥ) 

**Ακαδημαϊκό Έτος:** 2025-2026 

**Ομάδα:** 12

---

## 📋 Περιγραφή Project
Υλοποίηση του **Chordify**, μιας P2P εφαρμογής ανταλλαγής τραγουδιών βασισμένη στο πρωτόκολλο **Chord DHT**. Το σύστημα υποστηρίζει δυναμική εισαγωγή/αποχώρηση κόμβων, κατακερματισμό κλειδιών (hashing) και replication δεδομένων με διαφορετικά μοντέλα συνέπειας.

---

## 🚀 Progress Tracker / Roadmap

### Φάση 1: Επιλογές Αρχιτεκτονικής 
- [ ] **Επιλογή Γλώσσας** Επιλογή γλώσσας για την υλοποίηση της εφαρμογής (Go / Python κ.α.)
- [ ] **Επιλογή Βιβλιοθήκης** Επιλογή βιβλιοθηκών για την υλοποίηση της εφαρμογής

### Φάση 2: Βασική Υποδομή Κόμβου (Node Infrastructure)
- [ ] **Socket Setup:** Υλοποίηση server/client processes και multithreading για ταυτόχρονη εξυπηρέτηση αιτημάτων.
- [ ] **ID Generation:** Υλοποίηση SHA-1 hash function στο ζεύγος `ip_address:port` για παραγωγή μοναδικού ID.
- [ ] **Message Protocol:** Ορισμός μορφής μηνυμάτων για επικοινωνία μεταξύ κόμβων (Custom protocol).

### Φάση 3: Διαχείριση Μελών (Membership Management)
- [ ] **Bootstrap Node:** Υλοποίηση σταθερού κόμβου για την αρχική σύνδεση.
- [ ] **Node Join:** Υλοποίηση `join(nodeID)`. Ενημέρωση δεικτών και μεταφορά κλειδιών στον νέο κόμβο.
- [ ] **Graceful Departure:** Υλοποίηση `depart(nodeID)`. Ενημέρωση γειτόνων και ανακατανομή κλειδιών πριν την έξοδο.

### Φάση 4: Λειτουργίες DHT & Routing (Basic Chord)
- [ ] **Routing Logic:** Κάθε κόμβος κρατάει pointers για `successor` και `predecessor`.
- [ ] **Insert Operation:** Υλοποίηση `insert(key, value)`. Αν το key υπάρχει, γίνεται `concat` (update).
- [ ] **Query Operation:** Υλοποίηση `query(key)` για εντοπισμό value. Χειρισμός του wildcard `*` για επιστροφή όλων των κλειδιών.
- [ ] **Delete Operation:** Υλοποίηση `delete(key)`.
- [ ] **Hashing Logic:** Hashing του `key` (τίτλος τραγουδιού) για εύρεση υπεύθυνου κόμβου.
- [ ] *(Bonus)* **Finger Tables:** Υλοποίηση λογαριθμικής δρομολόγησης (προαιρετικό).

### Φάση 5: Replication & Consistency
- [ ] **Replication Strategy:** Αποθήκευση ζευγών `<key, value>` στον υπεύθυνο κόμβο και στους $k-1$ επόμενους.
- [ ] **Linearizability (Strong Consistency):**
  - [ ] Υλοποίηση μηχανισμού (Chain Replication ή Quorum).
  - [ ] Διαχείριση Read/Write με versions.
- [ ] **Eventual Consistency:**
  - [ ] Lazy propagation των writes στους replicas.
  - [ ] Reads από οποιονδήποτε replica (πιθανότητα stale data).

### Φάση 6: Client (CLI)
- [ ] **Help:** Εκτύπωση οδηγιών.
- [ ] **Insert:** `insert <key> <value>`.
- [ ] **Delete:** `delete <key>`.
- [ ] **Query:** `query <key>` (και υποστήριξη `query *`).
- [ ] **Depart:** `depart`.
- [ ] **Overlay:** `overlay` (Εκτύπωση τοπολογίας δακτυλίου).

### Φάση 7: Graphics (GUI) (optional)
- [ ] *(Bonus)* **GUI:** Υλοποίηση gui για την κλήση των commands όπως στο CLI

### Φάση 8: Πειράματα (AWS)
- [ ] **Setup:** Στήσιμο σε 10 κόμβους στο AWS.
- [ ] **Experiment 1 (Write Throughput):** Insert keys από αρχεία `insert_n.txt` με $k=1, 3, 5$ (Linear & Eventual).
- [ ] **Experiment 2 (Read Throughput):** Query keys από αρχεία `query_n.txt` με τα παραπάνω setups.
- [ ] **Experiment 3 (Consistency Check):** Εκτέλεση `requests.txt` και σύγκριση φρεσκάδας δεδομένων (Linear vs Eventual).

### Φάση 9: Αναφορά
- [ ] **Αναφορά:** Σύνθεση αναφοράς και σχολιασμός αποτελεσμάτων
