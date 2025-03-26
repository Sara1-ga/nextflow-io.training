# Part 2: Hello Channels

<div class="video-wrapper">
  <iframe width="560" height="315" src="https://www.youtube.com/embed/lJ41WMMm44M?si=xCItHLiOQWqoqBB9&amp;list=PLPZ8WHdZGxmXiHf8B26oB_fTfoKQdhlik" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
</div>

/// caption
:fontawesome-brands-youtube:{ .youtube } Guarda l'intera playlist su Nextflow YouTube channel](https://www.youtube.com/playlist?list=PLPZ8WHdZGxmXiHf8B26oB_fTfoKQdhlik).
///

Nella Parte 1 di questo corso (Hello World), ti abbiamo mostrato come fornire un input variabile a un processo specificando l'input direttamente nella chiamata al processo: `sayHello(params.greet)`.
Si è trattato di un approccio volutamente semplificato.
In pratica, questo approccio presenta notevoli limitazioni, in particolare perché funziona solo nei casi molto semplici in cui si desidera eseguire il processo una sola volta, su un singolo valore.
Nella maggior parte dei casi d'uso realistici del flusso di lavoro, vogliamo elaborare più valori (ad esempio, dati sperimentali per più campioni), quindi abbiamo bisogno di un modo più sofisticato per gestire gli input.
Ecco a cosa servono i **canali** di Nextflow.
I channels sono code progettate per gestire gli input in modo efficiente e spostarli da una fase all'altra nei flussi di lavoro multi-step, offrendo al contempo parallelismo integrato e molti altri vantaggi.
In questa parte del corso imparerai come utilizzare un canale per gestire più input provenienti da diverse fonti.
Imparerai anche ad usare gli  **operatori** per trasformare i contenuti del canale in base alle tue esigenze.

_Per una formazione sull'uso dei channels per collegare i passaggi in un flusso di lavoro multi-step, vedere la Parte 3 di questo corso._

---

## 0. Warmup: Run `hello-channels.nf`

Utilizzeremo lo script del flusso di lavoro `hello-channels.nf` come punto di partenza.
È equivalente allo script prodotto lavorando sulla Parte 1 di questo corso di formazione.

Per assicurarti che tutto funzioni, esegui lo script una volta prima di apportare modifiche:

```bash
nextflow run hello-channels.nf --greeting 'Hello Channels!'
```

```console title="Output"
 N E X T F L O W   ~  version 24.10.0

Launching `hello-channels.nf` [insane_lichterman] DSL2 - revision: c33d41f479

executor >  local (1)
[86/9efa08] sayHello | 1 of 1 ✔
```

Come in precedenza, troverete il file di output denominato `output.txt`nella `results` directory (specificata nella direttiva `publishDir` ).

```console title="results/output.txt" linenums="1"
Hello Channels!
```

Se tutto questo ha funzionato, sei pronto per imparare a usare i canali.

---

## 1. Fornire input variabili tramite un canale in modo esplicito

Creeremo un **canale** per passare l'input della variabile al processo `sayHello()` invece di affidarci alla gestione implicita, che presenta alcune limitazioni.

### 1.1. Creare un canale di input

Esistono diverse **fabbricazioni di canali** che possiamo usare per impostare un canale.
Per semplificare le cose per ora, useremo la fabbrica di  channels più elementare, chiamata `Channel.of`, che creerà un canale contenente un singolo valore.
Funzionalmente sarà simile a come lo avevamo impostato prima, ma invece di far creare un canale implicitamente a Nextflow, lo stiamo facendo esplicitamente ora.

Questa è la riga di codice che utilizzeremo:

```console title="Syntax"
greeting_ch = Channel.of('Hello Channels!')
```

Questo crea un canale denominato `greeting_ch` utilizzando il factory `Channel.of()`, che imposta un semplice canale di coda e carica la stringa ``Hello Channels!`` da utilizzare come valore di saluto.

!!! nota

  We are temporarily switching back to hardcoded strings instead of using a CLI parameter for the sake of readability. We'll go back to using CLI parameters once we've covered what's happening at the level of the channel.

Nel blocco del workflow, aggiungi il codice della factory del canale:

_Prima:_

```groovy title="hello-channels.nf" linenums="27"
workflow {

    // emit a greeting
    sayHello(params.greeting)
}
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="27"
workflow {

    // create a channel for inputs
    greeting_ch = Channel.of('Hello Channels!')

    // emit a greeting
    sayHello(params.greeting)
}
```

Questa operazione non è ancora funzionale poiché non abbiamo ancora commutato l'input nella chiamata di processo.

### 1.2. Aggiungere il canale come input alla chiamata di processo

Ora dobbiamo effettivamente collegare il nostro canale appena creato alla chiamata di processo `sayHello()`, sostituendo il parametro CLI che stavamo fornendo direttamente in precedenza.

Nel blocco del workflow, apportare la seguente modifica al codice:

_Prima:_

```groovy title="hello-channels.nf" linenums="27"
workflow {

    // create a channel for inputs
    greeting_ch = Channel.of('Hello Channels!')

    // emit a greeting
    sayHello(params.greeting)
}
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="27"
workflow {

    // create a channel for inputs
    greeting_ch = Channel.of('Hello Channels!')

    // emit a greeting
    sayHello(greeting_ch)
}
```

Questo indica a Nextflow di eseguire il processo `sayHello` sui contenuti del canale `greeting_ch`.

Ora il nostro workflow è correttamente funzionante; è l'equivalente esplicito della scrittura `sayHello('Hello Channels!')`.

### 1.3. Eseguire nuovamente il comando del workflow

Eseguamolo !

```bash
nextflow run hello-channels.nf
```

Se hai effettuato entrambe le modifiche correttamente, dovresti ottenere un'altra esecuzione riuscita:

```console title="Output" linenums="1"
 N E X T F L O W   ~  version 24.10.0

Launching `hello-channels.nf` [nice_heisenberg] DSL2 - revision: 41b4aeb7e9

executor >  local (1)
[3b/f2b109] sayHello (1) | 1 of 1 ✔
```

Puoi controllare la directory dei risultati per accertarti che il risultato sia ancora lo stesso di prima.

```console title="results/output.txt" linenums="1"
Hello Channels!
```

Finora stiamo solo modificando progressivamente il codice per aumentare la flessibilità del nostro flusso di lavoro, ottenendo sempre lo stesso risultato finale.

!!! nota

   This may seem like we're writing more code for no tangible benefit, but the value will become clear as soon as we start handling more inputs.

### Takeaway

Sai come utilizzare una channel factory di base per fornire un input a un processo.

### Cosa succede dopo?

Scopri come utilizzare i channels per far sì che il workflow esegua l'iterazione su più valori di input.

---

## 2. Modifica il workflow  per eseguire più valori di imput

I flussi di lavoro in genere vengono eseguiti su batch di input che devono essere elaborati in blocco, quindi vogliamo aggiornare il workflow in modo che accetti più valori di input.

### 2.1. Carica più saluti nel canale di input

Convenientemente, la factory di channels `Channel.of()` che abbiamo utilizzato è abbastanza felice di accettare più di un valore, quindi non abbiamo bisogno di modificarla affatto.
Dobbiamo solo caricare più valori nel canale.

#### 2.1.1. Aggiungi altri saluti

Nel blocco del workflow, apportare la seguente modifica al codice:

_Prima:_

```groovy title="hello-channels.nf" linenums="29"
// create a channel for inputs
greeting_ch = Channel.of('Hello Channels')
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="29"
// create a channel for inputs
greeting_ch = Channel.of('Hello','Bonjour','Holà')
```

La documentazione ci dice che dovrebbe funzionare. Può essere davvero così semplice?

#### 2.1.2.Esegui il comando e guarda l'output del registro

Proviamoci.
```bash
nextflow run hello-channels.nf
```

Sembra proprio che funzioni bene:

```console title="Output" linenums="1"
 N E X T F L O W   ~  version 24.10.0

Launching `hello-channels.nf` [suspicious_lamport] DSL2 - revision: 778deadaea

executor >  local (3)
[cd/77a81f] sayHello (3) | 3 of 3 ✔
```

Tuttavia... Questo sembra indicare che sono state effettuate '3 chiamate su 3' per il processo, il che è incoraggiante, ma questo ci mostra solo una singola esecuzione del processo, con un percorso di sottodirectory (`cd/77a81f`).
Cosa sta succedendo?

Di default, il sistema di registrazione ANSI scrive la registrazione da più chiamate allo stesso processo sulla stessa riga. 
Fortunatamente, possiamo disabilitare questo comportamento per vedere l'elenco completo delle chiamate di processo.
#### 2.1.3. Eseguire nuovamente il comando con l'opzione `-ansi-log false`

Per espandere la registrazione in modo da visualizzare una riga per chiamata di processo, aggiungere `-ansi-log false` al comando.

```bash
nextflow run hello-channels.nf -ansi-log false
```

Questa volta vediamo tutti e tre i processi eseguiti e le sottodirectory di lavoro associate elencate nell'output:

```console title="Output" linenums="1"
N E X T F L O W  ~  version 24.10.0
Launching `hello-channels.nf` [pensive_poitras] DSL2 - revision: 778deadaea
[76/f61695] Submitted process > sayHello (1)
[6e/d12e35] Submitted process > sayHello (3)
[c1/097679] Submitted process > sayHello (2)
```

Molto meglio, almeno per un workflow  semplice.
Per un workflow complesso o un gran numero di input, avere l'elenco completo in uscita sul terminale potrebbe risultare un po' opprimente, quindi potresti non scegliere di usare `-ansi-log false` in quei casi.
!!! nota

The way the status is reported is a bit different between the two logging modes.
In the condensed mode, Nextflow reports whether calls were completed successfully or not.
In this expanded mode, it only reports that they were submitted.
    
 Detto questo, abbiamo un altro problema. Se guardi nella directory `results`, c'è un solo file: `output.txt`!

```console title="Directory contents"
results
└── output.txt
```

Cosa succede? Non dovremmo aspettarci un file separato per ogni saluto di input, quindi tre file in tutto?
Tutti e tre i saluti sono andati in un singolo file?

Puoi controllare il contenuto di `output.txt`; ne troverai solo uno dei tre, contenente uno dei tre saluti che abbiamo fornito.

```console title="output.txt" linenums="1"
Bonjour
```

Potresti ricordare che abbiamo codificato in modo rigido il nome del file di output per il processo `sayHello`, quindi tutte e tre le chiamate hanno prodotto un file chiamato `output.txt`. 
Puoi controllare le sottodirectory di lavoro per ciascuno dei tre processi; ognuna di esse contiene un file chiamato `output.txt` come previsto.

Finché i file di output restano lì, isolati dagli altri processi, va bene. 
Ma quando la direttiva `publishDir` copia ciascuno di essi nella stessa directory `results`, quello che è stato copiato per primo viene sovrascritto dal successivo, e così via.

### 2.2. Assicurarsi che i nomi dei file di output siano univoci

Possiamo continuare a pubblicare tutti gli output nella stessa directory dei risultati, ma dobbiamo assicurarci che abbiano nomi univoci.
In particolare, dobbiamo modificare il primo processo per generare un nome file in modo dinamico, in modo che i nomi file finali siano univoci.

Quindi, come rendiamo univoci i nomi dei file?
Un modo comune per farlo è usare un pezzo univoco di metadati dagli input (ricevuti dal canale di input) come parte del nome del file di output.
Qui, per comodità, useremo semplicemente il saluto stesso, poiché è solo una stringa breve, e lo anteporremo al nome del file di output di base.

#### 2.2.1. Costruisci un nome di file di output dinamico

Nel blocco del processo, apportare le seguenti modifiche al codice:
_Prima:_

```groovy title="hello-channels.nf" linenums="6"
process sayHello {

    publishDir 'results', mode: 'copy'

    input:
        val greeting

    output:
        path 'output.txt'

    script:
    """
    echo '$greeting' > output.txt
    """
}
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="6"
process sayHello {

    publishDir 'results', mode: 'copy'

    input:
        val greeting

    output:
        path "${greeting}-output.txt"

    script:
    """
    echo '$greeting' > '$greeting-output.txt'
    """
}
```

Assicuratevi di sostituire `output.txt` sia nella definizione di output che nel blocco di comando `script:`.

!!! tip

   Nella definizione di output, DEVI utilizzare le virgolette doppie attorno all'espressione del nome del file di output (NON le virgolette singole), altrimenti l'operazione non andrà a buon fine.

Ciò dovrebbe produrre un nome di file di output univoco ogni volta che il processo viene chiamato, in modo che possa essere distinto dagli output di altre iterazioni dello stesso processo nella directory di output.

#### 2.2.2. Eseguire il flusso di lavoro

Facciamolo partire :

```bash
nextflow run hello-channels.nf
```

Tornando alla vista riepilogativa, l'output appare di nuovo così:

```console title="Output" linenums="1"
 N E X T F L O W   ~  version 24.10.0

Launching `hello-channels.nf` [astonishing_bell] DSL2 - revision: f57ff44a69

executor >  local (3)
[2d/90a2e2] sayHello (1) | 3 of 3 ✔
```

È importante notare che ora abbiamo tre nuovi file in aggiunta a quello che avevamo già nella directory `results`:

```console title="Directory contents"
results
├── Bonjour-output.txt
├── Hello-output.txt
├── Holà-output.txt
└── output.txt
```

Ognuno di essi ha i contenuti previsti:

```console title="Bonjour-output.txt" linenums="1"
Bonjour
```

```console title="Hello-output.txt" linenums="1"
Hello
```

```console title="Holà-output.txt" linenums="1"
Holà
```

Successo! Ora possiamo aggiungere tutti i saluti che vogliamo senza preoccuparci che i file di output vengano sovrascritti.

!!! nota

    In practice, naming files based on the input data itself is almost always impractical.
    The better way to generate dynamic filenames is to pass metadata to a process along with the input files.
    The metadata is typically provided via a 'sample sheet' or equivalents.
    You'll learn how to do that later in your Nextflow training.

### Takeaaway

Sai come alimentare più elementi di input attraverso un canale.

### Cosa c'è dopo ?

Impara a usare un operatore per trasformare il contenuto di un canale.

---

## 3. Utilizzare un operatore per trasformare il contenuto di un canale

In Nextflow, gli [operatori](https://www.nextflow.io/docs/latest/reference/operator.html) ci consentono di trasformare il contenuto di un canale.

Ti abbiamo appena mostrato come gestire più elementi di input che sono stati codificati direttamente nella factory del canale.
E se volessimo fornire questi input multipli in un formato diverso?

Ad esempio, immagina di impostare una variabile di input contenente un array di elementi come questo:
`greetings_array = ['Hello','Bonjour','Holà']`

Possiamo caricarlo nel nostro canale di output e aspettarci che funzioni? Scopriamolo.

### 3.1. Fornire un array di valori come input al canale

Il buon senso suggerisce che dovremmo essere in grado di passare semplicemente un array di valori invece di un singolo valore. Giusto?

#### 3.1.1. Impostare la variabile di input

Prendiamo la variabile `greetings_array` che abbiamo appena immaginato e rendiamola realtà aggiungendola al blocco del workflow :

_Prima:_

```groovy title="hello-channels.nf" linenums="27"
workflow {

    // create a channel for inputs
    greeting_ch = Channel.of('Hello','Bonjour','Holà')
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="27"
workflow {

    // declare an array of input greetings
    greetings_array = ['Hello','Bonjour','Holà']

    // create a channel for inputs
    greeting_ch = Channel.of('Hello','Bonjour','Holà')
```

#### 3.1.2 Imposta la matrice dei saluti come input per la fabbrica dei canali

Sostituiremo i valori `'Hello','Bonjour','Holà'` attualmente codificati nella factory dei channels con `greetings_array` che abbiamo appena creato.

Nel blocco del workflow , apporta la seguente modifica:
_Prima:_

```groovy title="hello-channels.nf" linenums="32"
    // create a channel for inputs
    greeting_ch = Channel.of('Hello','Bonjour','Holà')
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="32"
    // create a channel for inputs
    greeting_ch = Channel.of(greetings_array)
```

#### 3.1.3. Eseguire il workflow
Proviamo a eseguire questo:

```bash
nextflow run hello-channels.nf
```

Oh no! Nextflow genera un errore che inizia così:

```console title="Output" linenums="1"
 N E X T F L O W   ~  version 24.10.0

Launching `hello-channels.nf` [friendly_koch] DSL2 - revision: 97256837a7

executor >  local (1)
[22/57e015] sayHello (1) | 0 of 1
ERROR ~ Error executing process > 'sayHello (1)'

Caused by:
  Missing output file(s) `[Hello, Bonjour, Holà]-output.txt` expected by process `sayHello (1)`
```

Sembra che Nextflow abbia provato a eseguire una singola chiamata di processo, usando `[Hello, Bonjour, Holà]` come valore stringa, invece di usare le tre stringhe nell'array come valori separati.

Come facciamo a far sì che Nextflow scompatti l'array e carichi le singole stringhe nel canale?

### 3.2. Utilizzare un operatore per trasformare i contenuti del canale

Ed è qui che entrano in gioco gli **operatori**.

Se scorri l'[elenco degli operatori](https://www.nextflow.io/docs/latest/reference/operator.html) nella documentazione di Nextflow, troverai [`flatten()`](https://www.nextflow.io/docs/latest/reference/operator.html#flatten), che fa esattamente ciò di cui abbiamo bisogno: scompattare il contenuto di un array e restituirlo come elementi individuali.
!!! nota

    It is technically possible to achieve the same results by using a different channel factory, [`Channel.fromList`](https://nextflow.io/docs/latest/reference/channel.html#fromlist), which includes an implicit mapping step in its operation.
    Here we chose not to use that in order to demonstrate the use of an operator on a fairly simple use case.

#### 3.2.1 Aggiungere l'operatore `flatten()`

Per applicare l'operatore `flatten()` al nostro canale di input, lo aggiungiamo alla dichiarazione della factory del canale.

Nel blocco del workflow, apportare la seguente modifica al codice:

_Prima:_

```groovy title="hello-channels.nf" linenums="31"
    // create a channel for inputs
    greeting_ch = Channel.of(greetings_array)
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="31"
    // create a channel for inputs
    greeting_ch = Channel.of(greetings_array)
                         .flatten()
```

Qui abbiamo aggiunto l'operatore sulla riga successiva per una migliore leggibilità, ma puoi aggiungere operatori sulla stessa riga della fabbrica del canale se preferisci, in questo modo: `greeting_ch = Channel.of(greetings_array).flatten()`

#### 3.2.2. Aggiungere `view()` per ispezionare il contenuto del canale

Potremmo eseguirlo subito per testare se funziona, ma già che ci siamo, aggiungeremo anche un paio di operatori [`view()`](https://www.nextflow.io/docs/latest/reference/operator.html#view), che ci consentono di ispezionare il contenuto di un canale.
Puoi pensare a `view()` come a uno strumento di debug, come un'istruzione `print()` in Python, o il suo equivalente in altri linguaggi.

Nel blocco del workflow , apportare la seguente modifica al codice:

_Prima:_

```groovy title="hello-channels.nf" linenums="31"
    // create a channel for inputs
    greeting_ch = Channel.of(greetings_array)
                         .flatten()
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="31"
    // create a channel for inputs
    greeting_ch = Channel.of(greetings_array)
                         .view { greeting -> "Before flatten: $greeting" }
                         .flatten()
                         .view { greeting -> "After flatten: $greeting" }
```

Stiamo utilizzando un operatore _closure_ qui, ovvero le parentesi graffe.
Questo codice viene eseguito per ogni elemento nel canale.
Definiamo una variabile temporanea per il valore interno, qui chiamata `greeting` (potrebbe essere qualsiasi cosa).
Questa variabile viene utilizzata solo nell'ambito di quella chiusura.

In questo esempio, `$greeting` rappresenta ogni singolo elemento caricato in un canale.
!!! nota "Note on `$it`"

    In some pipelines you may see a special variable called `$it` used inside operator closures.
    This is an _implicit_ variable that allows a short-hand access to the inner variable,
    without needing to define it with a `->`.

    We prefer to be explicit to aid code clarity, as such the `$it` syntax is discouraged and will slowly be phased out of the Nextflow language.

#### 3.2.3. Eseguire il workflow

Infine, puoi provare a eseguire nuovamente il workflow !

```bash
nextflow run hello-channels.nf
```

Questa volta funziona E ci fornisce un'ulteriore panoramica di come appaiono i contenuti del canale prima e dopo aver eseguito l'operatore `flatten()`:

```console title="Output" linenums="1"
 N E X T F L O W   ~  version 24.10.0

Launching `hello-channels.nf` [tiny_elion] DSL2 - revision: 1d834f23d2

executor >  local (3)
[8e/bb08f3] sayHello (2) | 3 of 3 ✔
Before flatten: [Hello, Bonjour, Holà]
After flatten: Hello
After flatten: Bonjour
After flatten: Holà
```

Come puoi vedere, otteniamo una singola istruzione `Before flatten:` perché a quel punto il canale contiene un elemento, l'array originale.
Quindi otteniamo tre istruzioni `After flatten:` separate, una per ogni saluto, che ora sono elementi individuali nel canale.

Ciò significa che ogni elemento può ora essere elaborato separatamente dal workflow.

!!! tip

    You should delete or comment out the `view()` statements before moving on.

    ```groovy title="hello-channels.nf" linenums="31"
    // create a channel for inputs
    greeting_ch = Channel.of(greetings_array)
                         .flatten()
    ```

    We left them in the `hello-channels-3.nf` solution file for reference purposes.

### Takeaway

Sai come usare un operatore come `flatten()` per trasformare il contenuto di un canale e come usare l'operatore `view()` per ispezionare il contenuto del canale prima e dopo l'applicazione di un operatore.

### Cosa c'è dopo ?

Scopri come far sì che il workflow accetti un file come origine dei valori di input.

---

## 4. Utilizzare un operatore per analizzare i valori di input da un file CSV

Capita spesso che, quando vogliamo eseguire più input, i valori di input siano contenuti in un file.
Ad esempio, abbiamo preparato un file CSV chiamato `greetings.csv` contenente diversi saluti, uno su ogni riga (come una colonna di dati).

```csv title="greetings.csv" linenums="1"
Hello
Bonjour
Holà
```

Ora dobbiamo modificare il nostro workflow per leggere i valori da un file di questo tipo.

### 4.1. Modificare lo script per aspettarsi un file CSV come origine dei saluti

Per iniziare, dovremo apportare due modifiche chiave allo script:

- Cambiare il parametro di input per puntare al file CSV
- Passare a una fabbrica di channels progettata per gestire un file

#### 4.1.1. Cambia il parametro di input per puntare al file CSV

Ricordi il parametro `params.greeting` che abbiamo impostato nella Parte 1?
Lo aggiorneremo per puntare al file CSV contenente i nostri saluti.

Nel blocco del workflow , apportare la seguente modifica al codice:

_Prima:_

```groovy title="hello-channels.nf" linenums="25"
/*
 * Pipeline parameters
 */
params.greeting = ['Hello','Bonjour','Holà']
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="25"
/*
 * Pipeline parameters
 */
params.greeting = 'greetings.csv'
```

#### 4.1.2 Passare a una factory di channels progettata per gestire un file

Poiché ora vogliamo usare un file invece di semplici stringhe come input, non possiamo usare la factory di channels `Channel.of()` di prima. 
Dobbiamo passare all'uso di una nuova factory di canali, [`Channel.fromPath()`](https://www.nextflow.io/docs/latest/reference/channel.html#channel-path), che ha alcune funzionalità integrate per gestire i percorsi dei file.

Nel blocco del workflow, apportare la seguente modifica al codice:
_Prima:_

```groovy title="hello-channels.nf" linenums="31"
    // create a channel for inputs
    greeting_ch = Channel.of(greetings_array)
                         .flatten()
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="31"
    // create a channel for inputs from a CSV file
    greeting_ch = Channel.fromPath(params.greeting)
```

#### 4.1.3. Eseguire il workflow

Proviamo a eseguire il workflow con la nuova fabbrica di channels e il file di input.

```bash
nextflow run hello-channels.nf
```

Oh no, questo non funziona. Ecco l'inizio dell'output della console e del messaggio di errore:

```console title="Output" linenums="1"
 N E X T F L O W   ~  version 24.10.0

Launching `hello-channels.nf` [adoring_bhabha] DSL2 - revision: 8ce25edc39

[-        ] sayHello | 0 of 1
ERROR ~ Error executing process > 'sayHello (1)'

Caused by:
  File `/workspaces/training/hello-nextflow/data/greetings.csv-output.txt` is outside the scope of the process work directory: /workspaces/training/hello-nextflow/work/e3/c459b3c8f4029094cc778c89a4393d


Command executed:

  echo '/workspaces/training/hello-nextflow/data/greetings.csv' > '/workspaces/training/hello-nextflow/data/greetings.
```

In questo caso, il bit `Comando eseguito:` (righe 13-15) è particolarmente utile.

Questo potrebbe sembrare un po' familiare.
Sembra che Nextflow abbia provato a eseguire una singola chiamata di processo usando il percorso del file stesso come valore stringa.
Quindi ha risolto correttamente il percorso del file, ma in realtà non ne ha analizzato il contenuto, che è ciò che volevamo.

Come facciamo a far sì che Nextflow apra il file e ne carichi il contenuto nel canale?

Sembra che ci serva un altro [operatore](https://www.nextflow.io/docs/latest/reference/operator.html)!

### 4.2. Utilizzare l'operatore `splitCsv()` per analizzare il file

Esaminando nuovamente l'elenco degli operatori, troviamo [`splitCsv()`](https://www.nextflow.io/docs/latest/reference/operator.html#splitCsv), progettato per analizzare e dividere il testo in formato CSV.

#### 4.2.1. Applicare `splitCsv()` al canale

Per applicare l'operatore, lo aggiungiamo alla riga della factory del canale come in precedenza.

Nel blocco del workflow, apportiamo la seguente modifica al codice:

_Prima:_

```groovy title="hello-channels.nf" linenums="31"
// create a channel for inputs from a CSV file
greeting_ch = Channel.fromPath(params.greeting)
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="31"
// create a channel for inputs from a CSV file
greeting_ch = Channel.fromPath(params.greeting)
                     .view { csv -> "Before splitCsv: $csv" }
                     .splitCsv()
                     .view { csv -> "After splitCsv: $csv" }
```

Come puoi vedere, includiamo anche le istruzioni di visualizzazione prima/dopo.

#### 4.2.2. Eseguire nuovamente il workflow

Proviamo a eseguire il workflow con l'aggiunta della logica di analisi CSV.

```bash
nextflow run hello-channels.nf
```

È interessante notare che anche questo fallisce, ma con un errore diverso. L'output della console e l'errore iniziano così:

```console title="Output" linenums="1"
 N E X T F L O W   ~  version 24.10.0

Launching `hello-channels.nf` [stoic_ride] DSL2 - revision: a0e5de507e

executor >  local (3)
[42/8fea64] sayHello (1) | 0 of 3
Before splitCsv: /workspaces/training/hello-nextflow/greetings.csv
After splitCsv: [Hello]
After splitCsv: [Bonjour]
After splitCsv: [Holà]
ERROR ~ Error executing process > 'sayHello (2)'

Caused by:
  Missing output file(s) `[Bonjour]-output.txt` expected by process `sayHello (2)`


Command executed:

  echo '[Bonjour]' > '[Bonjour]-output.txt'
```

Questa volta Nextflow ha analizzato il contenuto del file (evviva!) ma ha aggiunto delle parentesi attorno ai saluti.

Per farla breve, `splitCsv()` legge ogni riga in un array e ogni valore separato da virgole nella riga diventa un elemento nell'array.
Quindi qui ci fornisce tre array contenenti un elemento ciascuno.

!!! nota

    Even if this behavior feels inconvenient right now, it's going to be extremely useful later when we deal with input files with multiple columns of data.

Potremmo risolvere questo problema usando `flatten()`, che già conosci. 
Tuttavia, esiste un altro operatore chiamato `map()` che è più appropriato da usare qui ed è davvero utile conoscerlo; compare spesso nelle pipeline di Nextflow.
### 4.3. Utilizzare l'operatore `map()` per estrarre i saluti

L'operatore `map()` è un piccolo strumento molto utile che ci consente di fare tutti i tipi di mappature al contenuto di un canale.

In questo caso, lo useremo per estrarre quell'elemento che vogliamo da ogni riga del nostro file.
Ecco come appare la sintassi:

```groovy title="Syntax"
.map { item -> item[0] }
```

Ciò significa "per ogni elemento nel canale, prendi il primo di tutti gli elementi in esso contenuti".

Applichiamolo alla nostra analisi CSV.

#### 4.3.1. Applicare `map()` al canale

Nel blocco del workflow , apportare la seguente modifica al codice:

_Prima:_

```groovy title="hello-channels.nf" linenums="31"
// create a channel for inputs from a CSV file
greeting_ch = Channel.fromPath(params.greeting)
                     .view { csv -> "Before splitCsv: $csv" }
                     .splitCsv()
                     .view { csv -> "After splitCsv: $csv" }
```

_Dopo:_

```groovy title="hello-channels.nf" linenums="31"
// create a channel for inputs from a CSV file
greeting_ch = Channel.fromPath(params.greeting)
                     .view { csv -> "Before splitCsv: $csv" }
                     .splitCsv()
                     .view { csv -> "After splitCsv: $csv" }
                     .map { item -> item[0] }
                     .view { csv -> "After map: $csv" }
```

Ancora una volta includiamo un'altra chiamata `view()` per confermare che l'operatore faccia ciò che ci aspettiamo.

#### 4.3.2. Eseguire il workflow ancora una volta

Ripetiamolo ancora una volta:

```bash
nextflow run hello-channels.nf
```

Questa volta dovrebbe funzionare senza errori.

```console title="Output" linenums="1"
 N E X T F L O W   ~  version 24.10.0

Launching `hello-channels.nf` [tiny_heisenberg] DSL2 - revision: 845b471427

executor >  local (3)
[1a/1d19ab] sayHello (2) | 3 of 3 ✔
Before splitCsv: /workspaces/training/hello-nextflow/greetings.csv
After splitCsv: [Hello]
After splitCsv: [Bonjour]
After splitCsv: [Holà]
After map: Hello
After map: Bonjour
After map: Holà
```

Osservando l'output delle istruzioni `view()`, vediamo quanto segue:

- Una singola istruzione `Before splitCsv:`: a quel punto il canale contiene un elemento, il percorso del file originale.
- Tre istruzioni separate `After splitCsv:`: una per ogni saluto, ma ciascuna è contenuta in un array che corrisponde a quella riga nel file.
- Tre istruzioni separate `After map:`: una per ogni saluto, che ora sono elementi individuali nel canale.
You can also look at the output files to verify that each greeting was correctly extracted and processed through the workflow.

!!! nota

    Here we had all greetings on one line in the CSV file.
    You can try adding more columns to the CSV file and see what happens; for example, try the following:

    ```csv title="greetings.csv"
    Hello,English
    Bonjour,French
    Holà,Spanish
    ```

    You can also try replacing `.map { item -> item[0] }` with `.flatten()` and see what happens depending on how many lines and columns you have in the input file.

    You'll learn learn more advanced approaches for handling complex inputs in a later training.

### Takeaway

Sai come usare gli operatori `splitCsv()` e `map()` per leggere un file di valori di input e gestirli in modo appropriato.

Più in generale, hai una conoscenza di base di come Nextflow usa i **canali** per gestire gli input nei processi e gli **operatori** per trasformarne i contenuti.

### Cosa c'è dopo ?

Fai una bella pausa, hai lavorato sodo in questo!
Quando sei pronto, passa alla Parte 3 per imparare come aggiungere altri passaggi e collegarli insieme in un flusso di lavoro appropriato.
