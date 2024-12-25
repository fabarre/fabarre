Option Explicit

On Error Resume Next

' Configurazione
Const dbServer = "localhost"
Const dbUser = "root"
Const dbPassword = "XXX"
Const dbName = "asm"
Const sourceFolder = "Z:\REPORTISTICA\Fatturato\ELE\DaCaricare"
Const destFolder = "Z:\REPORTISTICA\Fatturato\ELE\Caricati"
Const CHUNK_SIZE = 1048576 ' 1MB per chunk
Const BATCH_SIZE = 5000    ' Batch size ottimizzato
Const MAX_PACKET_SIZE = 1048576  ' 1MB limite massimo per pacchetto

' Variabili globali
Dim csvPath, logPath, logFile
Dim totalRows, skippedRows, csvRows
Dim currentPacketSize

' Log
Sub WriteLog(message)
    If Not logFile Is Nothing Then
        logFile.WriteLine Now & " - " & message
    End If
End Sub

' Funzione di supporto per la conversione dei numeri
Function ConvertToDecimal(strValue)
    strValue = Trim(strValue)
    If strValue = "" Then
        ConvertToDecimal = "NULL"
    Else
        strValue = Replace(Replace(strValue, " ", ""), ",", ".")
        ConvertToDecimal = strValue
    End If
End Function

' Funzione per convertire il formato data
Function ConvertToMySQLDate(dateStr)
    Dim parts, convertedDate
    dateStr = Trim(dateStr)
    If InStr(dateStr, "/") > 0 Then
        parts = Split(dateStr, "/")
        If UBound(parts) = 2 Then
            If Len(parts(2)) = 2 Then parts(2) = "20" & parts(2)
            convertedDate = parts(2) & "-" & Right("0" & parts(1), 2) & "-" & Right("0" & parts(0), 2)
            ConvertToMySQLDate = convertedDate
            Exit Function
        End If
    End If
    ConvertToMySQLDate = "NULL"
End Function

' Verifica file in uso
Function IsFileInUse(filePath)
    On Error Resume Next
    Dim fso, testFile
    Set fso = CreateObject("Scripting.FileSystemObject")
    Set testFile = fso.OpenTextFile(filePath, 2)
    If Err.Number <> 0 Then
        IsFileInUse = True
        Err.Clear
    Else
        IsFileInUse = False
        testFile.Close
    End If
    Set testFile = Nothing
    Set fso = Nothing
    On Error GoTo 0
End Function
' Trova CSV più recente
Function GetLatestCSVFile(folderPath)
    On Error Resume Next
    Dim fso, folder, file, latestFile, latestDate
    Set fso = CreateObject("Scripting.FileSystemObject")
    
    If Err.Number <> 0 Then
        WriteLog "Errore FSO: " & Err.Description
        GetLatestCSVFile = ""
        Exit Function
    End If
    
    If Not fso.FolderExists(folderPath) Then
        WriteLog "Cartella non trovata: " & folderPath
        GetLatestCSVFile = ""
        Exit Function
    End If
    
    Set folder = fso.GetFolder(folderPath)
    For Each file in folder.Files
        If UCase(Right(file.Name, 4)) = ".CSV" Then
            If latestFile Is Nothing Or file.DateLastModified > latestDate Then
                Set latestFile = file
                latestDate = file.DateLastModified
            End If
        End If
    Next
    
    If Not latestFile Is Nothing Then
        GetLatestCSVFile = latestFile.Path
        WriteLog "File trovato: " & latestFile.Path
    Else
        GetLatestCSVFile = ""
    End If
    
    Set folder = Nothing
    Set fso = Nothing
End Function

' Sposta file elaborato
Function MoveToProcessed(sourceFile)
    On Error Resume Next
    Err.Clear
    
    WriteLog "Inizio processo di spostamento file..."
    WriteLog "File sorgente: " & sourceFile
    WriteLog "Cartella destinazione: " & destFolder
    
    Dim fso, fileName, destPath
    Set fso = CreateObject("Scripting.FileSystemObject")
    
    If Not fso.FileExists(sourceFile) Then
        WriteLog "ERRORE: File sorgente non trovato: " & sourceFile
        MoveToProcessed = False
        Exit Function
    End If
    
    fileName = fso.GetFileName(sourceFile)
    destPath = destFolder & "\" & fileName
    
    If fso.FileExists(destPath) Then
        destPath = destFolder & "\" & _
                  Left(fileName, Len(fileName)-4) & "_" & _
                  Replace(Replace(Replace(Now(), ":", ""), " ", "_"), "/", "") & ".csv"
    End If
    
    WScript.Sleep 2000
    
    fso.MoveFile sourceFile, destPath
    
    If Err.Number <> 0 Then
        WriteLog "ERRORE durante lo spostamento: " & Err.Description
        WriteLog "Tentativo di copia e cancellazione..."
        Err.Clear
        
        fso.CopyFile sourceFile, destPath, True
        If Err.Number = 0 Then
            If fso.FileExists(destPath) Then
                If fso.GetFile(sourceFile).Size = fso.GetFile(destPath).Size Then
                    fso.DeleteFile sourceFile, True
                    If Err.Number = 0 Then
                        WriteLog "File spostato con successo tramite copia e cancellazione"
                        MoveToProcessed = True
                    Else
                        WriteLog "ERRORE durante l'eliminazione del file sorgente: " & Err.Description
                        MoveToProcessed = False
                    End If
                Else
                    WriteLog "ERRORE: Le dimensioni dei file non corrispondono"
                    fso.DeleteFile destPath, True
                    MoveToProcessed = False
                End If
            End If
        Else
            WriteLog "ERRORE durante la copia: " & Err.Description
            MoveToProcessed = False
        End If
    Else
        WriteLog "File spostato con successo in: " & destPath
        MoveToProcessed = True
    End If
    
    Set fso = Nothing
End Function
' Elaborazione principale
Function ProcessCSVFile()
    On Error Resume Next
    ProcessCSVFile = False  ' Default a False
    
    Dim fso, stream, conn, rs
    Dim headerLine, dataLine, fields, columns(), createTableSQL, insertSQL
    Dim cleanName, cleanValue, i
    Dim startTime, endTime, imponibileColumn
    Dim batchValues, batchCount
    Dim j
    
    Set fso = CreateObject("Scripting.FileSystemObject")
    
    startTime = Now
    WriteLog "=== INIZIO IMPORTAZIONE ==="
    WriteLog "Data e ora inizio: " & startTime
    WriteLog "File CSV selezionato: " & csvPath

    ' Connessione ottimizzata
    WriteLog "Tentativo di connessione al database..."
    Set conn = CreateObject("ADODB.Connection")
    conn.Open "Driver={MySQL ODBC 8.0 Unicode Driver};Server=" & dbServer & ";Database=" & dbName & ";User=" & dbUser & ";Password=" & dbPassword & ";"
    If Err.Number <> 0 Then
        WriteLog "ERRORE: Connessione al database fallita: " & Err.Description
        ProcessCSVFile = False
        Exit Function
    End If
    WriteLog "Connessione al database stabilita con successo"

    ' Ottimizzazioni MySQL
    WriteLog "Configurazione ottimizzazioni MySQL..."
    conn.Execute "SET autocommit=0"
    conn.Execute "SET unique_checks=0"
    conn.Execute "SET foreign_key_checks=0"
    conn.Execute "SET sql_log_bin=0"
    conn.Execute "SET SESSION wait_timeout = 28800"
    conn.Execute "SET SESSION interactive_timeout = 28800"
    
    If Err.Number <> 0 Then
        WriteLog "ERRORE: Configurazione MySQL fallita: " & Err.Description
        ProcessCSVFile = False
        Exit Function
    End If

    ' Leggi file CSV
    WriteLog "Apertura file CSV..."
    Set stream = CreateObject("ADODB.Stream")
    stream.Type = 2 'Text
    stream.Charset = "UTF-8"
    stream.Open
    stream.LoadFromFile csvPath
    
    If Err.Number <> 0 Then
        WriteLog "ERRORE: Lettura file CSV fallita: " & Err.Description
        ProcessCSVFile = False
        Exit Function
    End If

    ' Leggi intestazione
    WriteLog "Lettura intestazione CSV..."
    headerLine = stream.ReadText(-2)
    fields = Split(headerLine, ";")
    ReDim columns(UBound(fields))

    ' Crea tabella temporanea
    WriteLog "Creazione tabella temporanea..."
    createTableSQL = "CREATE TABLE IF NOT EXISTS ele ("
    For i = 0 To UBound(fields)
        cleanName = Trim(fields(i))
        cleanName = Replace(cleanName, """", "")
        cleanName = Replace(cleanName, "'", "")
        
        Select Case cleanName
            Case "PREZZO €"
                cleanName = "PREZZO"
            Case "IMPONIBILE (€)"
                cleanName = "IMPONIBILE"
            Case Else
                cleanName = Replace(Replace(Replace(Replace(Replace(Replace(Replace(Replace(Replace(cleanName, " ", "_"), ".", "_"), "-", "_"), "/", "_"), "\", "_"), "(", "_"), ")", "_"), "[", "_"), "]", "_")
                cleanName = Replace(cleanName, "€", "EUR")
        End Select
        
        If cleanName = "" Then
            cleanName = "Colonna_" & (i + 1)
            WriteLog "ATTENZIONE: Trovata colonna senza nome in posizione " & (i + 1) & ". Rinominata in '" & cleanName & "'"
        End If
        
        columns(i) = cleanName
        ' Se è una colonna numerica
        If InStr(cleanName, "IMPONIBILE") > 0 Or InStr(cleanName, "PREZZO") > 0 Then
            createTableSQL = createTableSQL & "`" & cleanName & "` DECIMAL(14,6)"
        Else
            createTableSQL = createTableSQL & "`" & cleanName & "` TEXT"
        End If
        If i < UBound(fields) Then createTableSQL = createTableSQL & ", "
    Next
    createTableSQL = createTableSQL & ") CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"

    conn.Execute "DROP TABLE IF EXISTS ele"
    conn.Execute createTableSQL
    
    If Err.Number <> 0 Then
        WriteLog "ERRORE: Creazione tabella temporanea fallita: " & Err.Description
        ProcessCSVFile = False
        Exit Function
    End If
    WriteLog "Tabella temporanea creata con successo"
    
    conn.Execute "START TRANSACTION"

    ' Crea indice sulla tabella temporanea
    conn.Execute "ALTER TABLE ele ADD INDEX idx_voce_tariffa (VoceTariffa)"

    ' Prepara l'inserimento dati
    insertSQL = "INSERT INTO ele (`" & Join(columns, "`, `") & "`) VALUES "

' Legge e inserisce i dati
    WriteLog "Inizio inserimento dati..."
    totalRows = 0
    skippedRows = 0
    csvRows = 0
    currentPacketSize = 0
    batchValues = ""
    batchCount = 0

    WriteLog "Inizio lettura dati dal file CSV..."
    Do Until stream.EOS
        dataLine = stream.ReadText(-2)
        csvRows = csvRows + 1
        
        If Len(Trim(dataLine)) > 0 Then
            fields = Split(dataLine, ";")
            
            If UBound(fields) = UBound(columns) Then
                Dim insertValues : insertValues = "("
                
                For i = 0 To UBound(fields)
                    cleanValue = Trim(fields(i))
                    If cleanValue = "" Or cleanValue = " " Then
                        insertValues = insertValues & "NULL"
                    Else
                        ' Se è una colonna numerica
                        If InStr(columns(i), "IMPONIBILE") > 0 Or InStr(columns(i), "PREZZO") > 0 Then
                            cleanValue = Replace(Replace(cleanValue, " ", ""), ",", ".")
                            insertValues = insertValues & cleanValue
                        Else
                            cleanValue = Replace(cleanValue, "\", "\\")
                            cleanValue = Replace(cleanValue, "'", "\'")
                            cleanValue = Replace(cleanValue, """", "\""")
                            insertValues = insertValues & "'" & cleanValue & "'"
                        End If
                    End If
                    If i < UBound(fields) Then insertValues = insertValues & ", "
                Next
                
                insertValues = insertValues & ")"
                currentPacketSize = currentPacketSize + Len(insertValues)
                batchValues = batchValues & insertValues & ","
                batchCount = batchCount + 1
                totalRows = totalRows + 1
                
                If currentPacketSize >= MAX_PACKET_SIZE Or batchCount >= BATCH_SIZE Then
                    On Error Resume Next
                    WriteLog "Esecuzione batch di inserimento... Righe totali: " & totalRows
                    conn.Execute insertSQL & Left(batchValues, Len(batchValues) - 1)
                    If Err.Number <> 0 Then
                        WriteLog "ERRORE Batch: " & Err.Description
                        skippedRows = skippedRows + batchCount
                        ProcessCSVFile = False
                        Exit Function
                    End If
                    On Error GoTo 0
                    
                    WriteLog "Batch completato con successo"
                    batchValues = ""
                    batchCount = 0
                    currentPacketSize = 0
                    WScript.StdOut.Write vbCr & "Righe elaborate: " & totalRows
                    
                    If totalRows Mod 100000 = 0 Then
                        WriteLog "Esecuzione commit intermedio..."
                        conn.Execute "COMMIT"
                        conn.Execute "START TRANSACTION"
                        
                        ' Pulizia memoria
                        WriteLog "Pulizia memoria in corso..."
                        Set stream = Nothing
                        Set stream = CreateObject("ADODB.Stream")
                        stream.Type = 2
                        stream.Charset = "UTF-8"
                        stream.Open
                        stream.LoadFromFile csvPath
                        For j = 1 to totalRows
                            stream.ReadText(-2)
                        Next
                        WriteLog "Pulizia memoria completata"
                    End If
                End If
            Else
                WriteLog "ATTENZIONE: Riga " & csvRows & " saltata - numero campi non corretto"
                skippedRows = skippedRows + 1
            End If
        Else
            WriteLog "ATTENZIONE: Riga " & csvRows & " saltata - riga vuota"
            skippedRows = skippedRows + 1
        End If
    Loop

    ' Inserisce le righe rimanenti
    If batchCount > 0 Then
        On Error Resume Next
        WriteLog "Esecuzione batch finale..."
        conn.Execute insertSQL & Left(batchValues, Len(batchValues) - 1)
        If Err.Number <> 0 Then
            WriteLog "ERRORE Batch finale: " & Err.Description
            skippedRows = skippedRows + batchCount
            ProcessCSVFile = False
            Exit Function
        End If
        WriteLog "Batch finale completato con successo"
        On Error GoTo 0
    End If

    ' Verifica che i dati siano stati effettivamente inseriti nella tabella ele
    WriteLog "Verifica inserimento dati nella tabella temporanea..."
    Dim checkEleRS : Set checkEleRS = conn.Execute("SELECT COUNT(*) as cnt FROM ele")
    If checkEleRS("cnt") = 0 Then
        WriteLog "ERRORE: Nessun dato inserito nella tabella temporanea ele"
        ProcessCSVFile = False
        Exit Function
    End If
    WriteLog "Verifica completata: " & checkEleRS("cnt") & " righe inserite nella tabella temporanea"

' Trova la colonna IMPONIBILE
    WriteLog "Ricerca colonna IMPONIBILE..."
    For i = 0 To UBound(columns)
        If InStr(columns(i), "IMPONIBILE") > 0 Then
            imponibileColumn = columns(i)
            WriteLog "Trovata colonna IMPONIBILE: " & imponibileColumn
            Exit For
        End If
    Next

    If imponibileColumn = "" Then
        WriteLog "ERRORE: Colonna IMPONIBILE non trovata"
        ProcessCSVFile = False
        Exit Function
    End If

    ' Aggiunge e popola colonne addizionali
    WriteLog "Creazione e popolamento colonne aggiuntive..."
    WriteLog "Aggiornamento CODICE_TARIFFA e CODICE_VOCE..."
    conn.Execute "UPDATE ele SET CODICE_TARIFFA = REPLACE(CODICE_TARIFFA, ' ', '_')"
    conn.Execute "UPDATE ele SET CODICE_VOCE = REPLACE(CODICE_VOCE, ' ', '_')"
    
    WriteLog "Creazione colonna VoceTariffa..."
    conn.Execute "ALTER TABLE ele ADD COLUMN VoceTariffa TEXT"
    WriteLog "Popolamento VoceTariffa..."
    conn.Execute "UPDATE ele SET VoceTariffa = CASE " & _
                 "WHEN CODICE_VOCE IS NULL AND CODICE_TARIFFA IS NULL THEN NULL " & _
                 "ELSE CONCAT(COALESCE(TRIM(CODICE_VOCE),''), '_', COALESCE(TRIM(CODICE_TARIFFA),'')) " & _
                 "END"
                 
    If Err.Number <> 0 Then
        WriteLog "ERRORE durante l'aggiornamento di CODICE_TARIFFA e CODICE_VOCE: " & Err.Description
        ProcessCSVFile = False
        Exit Function
    End If

    WriteLog "Creazione colonna Consumi..."
    conn.Execute "ALTER TABLE ele ADD COLUMN Consumi DECIMAL(14,6)"
    WriteLog "Popolamento Consumi..."
    conn.Execute "UPDATE ele SET Consumi = " & _
        "CASE WHEN VoceTariffa IN (" & _
        "'E-B20-SPMA-E_P.U.N_ORARIO', " & _
        "'E-B20-SPMA-E_PRZFIS', " & _
        "'E-B20-SPMA-E_P.U.N.', " & _
        "'E-B20-SPMA-E_P.U.N._BIO', " & _
        "'E-B20-SPMA-E_PEBIO', " & _
        "'E-B20-SPMA-E_PEFME') " & _
        "THEN CAST(REPLACE(QUANTITA_TOTALE, ' ', '') AS DECIMAL(14,6)) " & _
        "ELSE 0 END"

    If Err.Number <> 0 Then
        WriteLog "ERRORE durante l'aggiornamento dei Consumi: " & Err.Description
        ProcessCSVFile = False
        Exit Function
    End If

    ' Commit delle modifiche alla tabella temporanea
    WriteLog "Esecuzione commit delle modifiche..."
    conn.Execute "COMMIT"
    conn.Execute "START TRANSACTION"

    ' Disabilita indici
    WriteLog "Disabilitazione indici..."
    conn.Execute "ALTER TABLE ele_pivot DISABLE KEYS"

    ' Prepara le VoceTariffa per il pivot
    WriteLog "Preparazione struttura colonne per il pivot..."
    Dim pivotColumns : Set pivotColumns = CreateObject("Scripting.Dictionary")
    WriteLog "Recupero VoceTariffa distinte..."
    Set rs = conn.Execute("SELECT DISTINCT VoceTariffa FROM ele WHERE VoceTariffa IS NOT NULL AND VoceTariffa <> '' ORDER BY VoceTariffa")
    
    If Err.Number <> 0 Then
        WriteLog "ERRORE durante la selezione delle VoceTariffa: " & Err.Description
        ProcessCSVFile = False
        Exit Function
    End If

    Do While Not rs.EOF
        Dim voceTariffa : voceTariffa = Trim(rs("VoceTariffa"))
        If Len(voceTariffa) > 0 Then
            pivotColumns.Add voceTariffa, True
            WriteLog "VoceTariffa trovata: " & voceTariffa
        End If
        rs.MoveNext
    Loop

    ' Verifica se abbiamo trovato delle VoceTariffa
    If pivotColumns.Count = 0 Then
        WriteLog "ERRORE: Nessuna VoceTariffa trovata"
        ProcessCSVFile = False
        Exit Function
    End If
    WriteLog "Trovate " & pivotColumns.Count & " VoceTariffa distinte"

' Prepara le stringhe per la query
    Dim insertColumns, selectColumns, columnName
    insertColumns = ""
    selectColumns = ""

    ' Usa un recordset forward-only per efficienza
    WriteLog "Lettura struttura tabella ele_pivot..."
    Set rs = conn.Execute("SHOW COLUMNS FROM ele_pivot")
    
    If Err.Number <> 0 Then
        WriteLog "ERRORE durante la lettura delle colonne di ele_pivot: " & Err.Description
        ProcessCSVFile = False
        Exit Function
    End If

    WriteLog "Costruzione query pivot..."
    Do While Not rs.EOF
        columnName = rs("Field")
        If Len(insertColumns) > 0 Then 
            insertColumns = insertColumns & ", "
            selectColumns = selectColumns & ", "
        End If
        
        insertColumns = insertColumns & "`" & columnName & "`"
        
        Select Case columnName
            Case "DATA_BOLLETTA"
                selectColumns = selectColumns & _
                    "CASE WHEN DATA_BOLLETTA REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' " & _
                    "THEN DATE_FORMAT(STR_TO_DATE(DATA_BOLLETTA, '%d/%m/%Y'), '%Y-%m-%d') " & _
                    "WHEN DATA_BOLLETTA REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' " & _
                    "THEN DATE_FORMAT(STR_TO_DATE(DATA_BOLLETTA, '%d/%m/%y'), '%Y-%m-%d') " & _
                    "ELSE NULL END"
            Case "Consumi"
                selectColumns = selectColumns & "CAST(SUM(Consumi) AS DECIMAL(14,6))"
            Case Else
                If pivotColumns.Exists(columnName) Then
                    selectColumns = selectColumns & _
                        "CAST(SUM(CASE WHEN VoceTariffa = '" & columnName & "' " & _
                        "THEN CAST(REPLACE(REPLACE(" & imponibileColumn & ", ' ', ''), ',', '.') AS DECIMAL(14,6)) " & _
                        "ELSE 0 END) AS DECIMAL(14,6))"
                Else
                    If InStr(1, "CODICE_CONTRATTO,RAG__SOCIALE,NUMERO_BOLLETTA,TIPO_UTILIZZO,MERCATO,DISTRIBUTORE,PDR,COMUNE,PROVINCIA,MESE_COMPETENZA", columnName) > 0 Then
                        selectColumns = selectColumns & "`" & columnName & "`"
                    Else
                        selectColumns = selectColumns & "CAST(0 AS DECIMAL(14,6))"
                    End If
                End If
        End Select
        
        rs.MoveNext
    Loop

    ' Costruisci la query finale con ottimizzazione della memoria
    WriteLog "Creazione query finale..."
    Dim finalSQL : finalSQL = "INSERT INTO ele_pivot (" & insertColumns & ") " & _
              "SELECT " & selectColumns & " FROM ele " & _
              "GROUP BY CODICE_CONTRATTO, RAG__SOCIALE, DATA_BOLLETTA, " & _
              "NUMERO_BOLLETTA, TIPO_UTILIZZO, MERCATO, " & _
              "DISTRIBUTORE, PDR, COMUNE, PROVINCIA, MESE_COMPETENZA"

    ' Esegui la query finale con gestione ottimizzata
    WriteLog "Esecuzione query pivot..."
    On Error Resume Next
    conn.Execute finalSQL
    If Err.Number <> 0 Then
        WriteLog "ERRORE durante il pivot: " & Err.Description & vbCrLf & "Query: " & finalSQL
        ProcessCSVFile = False
        Exit Function
    End If
    WriteLog "Query pivot eseguita con successo"
    On Error GoTo 0

    ' Verifica che i dati siano stati effettivamente inseriti
    WriteLog "Verifica inserimento dati in ele_pivot..."
    Dim checkRS : Set checkRS = conn.Execute("SELECT COUNT(*) as cnt FROM ele_pivot")
    If checkRS("cnt") = 0 Then
        WriteLog "ERRORE: Nessun dato inserito in ele_pivot"
        ProcessCSVFile = False
        Exit Function
    End If
    WriteLog "Dati inseriti correttamente in ele_pivot: " & checkRS("cnt") & " righe"

    ' Commit finale e pulizia
    WriteLog "Esecuzione commit finale..."
    conn.Execute "COMMIT"

    ' Pulizia e ripristino
    WriteLog "Ripristino configurazione database..."
    conn.Execute "ALTER TABLE ele_pivot ENABLE KEYS"
    conn.Execute "SET autocommit=1"
    conn.Execute "SET unique_checks=1"
    conn.Execute "SET foreign_key_checks=1"
    conn.Execute "SET sql_log_bin=1"
    conn.Execute "ANALYZE TABLE ele_pivot"

    If Err.Number <> 0 Then
        WriteLog "ERRORE durante il ripristino della configurazione: " & Err.Description
    End If

    WriteLog "Eliminazione tabella temporanea..."
    conn.Execute "DROP TABLE IF EXISTS ele"
    
    If Err.Number = 0 Then
        WriteLog "Elaborazione completata con successo"
        ProcessCSVFile = True  ' Imposta il successo solo se non ci sono stati errori
    End If

    ' Chiusura risorse
    If Not stream Is Nothing Then
        stream.Close
        Set stream = Nothing
    End If

    If Not conn Is Nothing Then
        If conn.State = 1 Then conn.Close
        Set conn = Nothing
    End If

    ' Pulizia finale memoria
    Set rs = Nothing
    Set fso = Nothing
    Set pivotColumns = Nothing

    endTime = Now
    WriteLog "=== RIEPILOGO IMPORTAZIONE ==="
    WriteLog "File elaborato: " & csvPath
    WriteLog "Righe elaborate: " & csvRows
    WriteLog "Righe saltate: " & skippedRows
    WriteLog "Righe inserite con successo: " & totalRows
    WriteLog "Tempo impiegato: " & DateDiff("s", startTime, endTime) & " secondi"
End Function

' SCRIPT PRINCIPALE
Dim fso
Set fso = CreateObject("Scripting.FileSystemObject")

Do
    csvPath = GetLatestCSVFile(sourceFolder)
    If csvPath = "" Then
        If Not logFile Is Nothing Then
            WriteLog "Nessun file CSV da elaborare. Uscita..."
        End If
        Exit Do
    End If
    
    ' Inizializza il log se non esiste
    logPath = Left(csvPath, InStrRev(csvPath, "\")) & "import_log_ele.txt"
    If Not fso.FileExists(logPath) Then
        Set logFile = fso.CreateTextFile(logPath, True)
    Else
        Set logFile = fso.OpenTextFile(logPath, 8, True)  ' 8 = ForAppending
    End If
    
    ' Esegui l'elaborazione e verifica il successo
    If ProcessCSVFile Then
        ' Sposta il file solo se l'elaborazione è riuscita
        If Not MoveToProcessed(csvPath) Then
            WriteLog "ATTENZIONE: Il file non è stato spostato correttamente, verrà rielaborato al prossimo ciclo"
        End If
    Else
        WriteLog "ERRORE: Elaborazione del file non riuscita, il file non verrà spostato"
    End If
    
    ' Pulizia memoria
    Set stream = Nothing
    Set conn = Nothing
    Set rs = Nothing
    CollectGarbage     ' Forza la pulizia della memoria
    
    WScript.Sleep 2000
Loop

' Chiusura finale
If Not logFile Is Nothing Then
    logFile.Close
    Set logFile = Nothing
End If

Set fso = Nothing
WScript.Quit
