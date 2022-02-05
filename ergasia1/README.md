### Documentation
#### Εργασία 1
##### Μέλη της ομάδας
* Αποστολάτος Ιωάννης ΑΜ:1115201900012
* Θεμελής Αλέξανδρος ΑΜ:1115201900062
* Ζαχαρόπουλος Γεώργιος-Κωνσταντίνος ΑΜ:1115201900061
-----
##### Εκτέλεση της εργασίας
* Για να εκτέσετε την εργασία τρέξτε `make ht` και μετά `./build/runner` ή `make run`.
* Για να ξανατρέξουμε την εργασία θα πρέπει να κάνουμε **πάντα** `make clean`. Στην ουσία θέλουμε να διαγράφουμε το data.db κάθε φορά.
-----
##### Παραδοχές
* Στο info block αποθηκεύουμε μόνο το global depth και βρίσκεται στη 1η θέση στον πίνακα των blocks.
* Το directory αποτελείται από 1 BF block και βρίσκεται στη 2η θέση στον πίνακα των blocks.
* Κάθε bucket είναι ένα block και έχει ως max χώρο 8 θέσεις για records. Στην αρχή του bucket αποθηκεύουμε το local depth και το counter των records. Τα buckets ξεκινούν πάντα μετά την 2η θέση του πίνακα των blocks.
* Τα offsets που χρησιμοποιούνται είναι τα εξής:
    * Για να διαβάσουμε ένα αριθμό από block χρειάζονται 5 θέσεις char.
    * Για τα δεδομένα του record έχουμε ότι id απαιτεί 5 θέσεις, το name απαιτεί 15 θέσεις, το surname 20 θέσεις, και το city 20 θέσεις. Συνολικά το record χρειάζεται 60 θέσεις char.
* Ο κώδικας μπορεί να διαχειριστεί μέχρι και 256 records. Kαθώς το directory έχει max 32 θέσεις για κλειδιά και ο χώρος του block δεν επιτρέπει άλλη επέκταση. Αμα προσπαθήσουμε να αποθηκεύσουμε παραπάνω παίρνουμε μήνυμα λάθους και το πρόγραμμα τερματίζει. Παρατηρούμε ότι η μέγιστη τιμή του global depth θα είναι 5.
* Ο πίνακας των open files είναι global.
* Η hash function μετaτρέπει το id σε binary και έπειτα επιστρέφει όσα bits επιτρέπει το global depth.
-----
##### Παρατηρήσεις-Σχόλια
* Η δομή του επεκτατού κατακερματισμού στην εργασία μας είναι λειτουργεί ορθά έως και τις 256 εγγραφές αλλά ο περιορισμός του μεγέθους του block (512) δεν επιτρέπει την περαιτέρο επέκτασή του.
* Ολες οι λειτουργίες του προγράμματός μας επιδεικνύονται στην ht_main.c
