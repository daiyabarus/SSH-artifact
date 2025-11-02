Attribute VB_Name = "Module5"
Sub AutoFillDay4G()
'
' Macro untuk AutoFill data di sheet Day 4G dari row 2 sampai end row
' Metode baru: Deteksi baris pertama yang blank, kemudian AutoFill dari situ sampai end
' Lebih efisien dan cepat
'
    Dim ws As Worksheet
    Dim sourceRange As Range
    Dim lastRow As Long
    Dim fillRange As Range
    Dim firstBlankRow As Long
    Dim checkRow As Long
    Dim isRowBlank As Boolean
    Dim col As Long
    Dim startCol As Long, endCol As Long
    
    ' Error handling
    On Error GoTo ErrorHandler
    
    ' Set worksheet Day 4G
    Set ws = ThisWorkbook.Sheets("Day 4G")
    
    ' Mencari baris terakhir yang berisi data (berdasarkan kolom A atau kolom pertama yang relevan)
    lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).row
    
    ' Jika tidak ada data atau hanya header, cari di kolom lain
    If lastRow < 2 Then
        lastRow = ws.Cells(ws.Rows.Count, "B").End(xlUp).row
        If lastRow < 2 Then
            lastRow = ws.Cells(ws.Rows.Count, "C").End(xlUp).row
        End If
    End If
    
    ' Pastikan minimal ada 2 baris (header + 1 data)
    If lastRow < 2 Then
        Exit Sub
    End If
    
    ' Disable screen updating untuk performa
    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual
    
    ' Definisi kolom start dan end (AX = kolom 50, BC = kolom 55)
    startCol = 50  ' AX
    endCol = 55    ' BC
    
    ' Cari baris pertama yang blank di range AX:BC
    firstBlankRow = 0
    For checkRow = 3 To lastRow  ' Mulai dari baris 3 (baris 2 adalah source)
        isRowBlank = True
        
        ' Periksa apakah semua cell di range AX:BC pada baris ini kosong
        For col = startCol To endCol
            If Not IsEmpty(ws.Cells(checkRow, col).Value) And ws.Cells(checkRow, col).Value <> "" Then
                isRowBlank = False
                Exit For
            End If
        Next col
        
        ' Jika menemukan baris yang blank, simpan dan keluar dari loop
        If isRowBlank Then
            firstBlankRow = checkRow
            Exit For
        End If
    Next checkRow
    
    ' Jika tidak ada baris blank, berarti semua sudah terisi
    If firstBlankRow = 0 Then
        GoTo RestoreSettings
    End If
    
    ' Set source range (baris 2 dari kolom AX sampai BC)
    Set sourceRange = ws.Range("AX2:BC2")
    
    ' Set destination range (dari firstBlankRow sampai lastRow)
    Set fillRange = ws.Range("AX" & firstBlankRow & ":BC" & lastRow)
    
    ' Lakukan AutoFill dari source ke destination
    sourceRange.AutoFill Destination:=ws.Range("AX2:BC" & lastRow)
    
RestoreSettings:
    ' Restore settings
    Application.ScreenUpdating = True
    Application.Calculation = xlCalculationAutomatic
    
    Exit Sub
    
ErrorHandler:
    Application.ScreenUpdating = True
    Application.Calculation = xlCalculationAutomatic
    Debug.Print "Error in AutoFillDay4G: " & Err.Description
End Sub

Sub RemoveDuplicatesFromTables()
    '
    ' RemoveDuplicatesFromTables Macro
    ' Removes duplicates from multiple data tables across worksheets
    '
    
    Dim ws As Worksheet
    Dim tblRange As Range
    
    ' Turn off screen updating for better performance
    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual
    
    On Error GoTo ErrorHandler
    
    ' Process KQI sheet - Remove duplicates from all 23 columns
    Set ws = ThisWorkbook.Sheets("KQI")
    Set tblRange = ws.Range("Tb_KQI[#All]")
    
    With tblRange
        .RemoveDuplicates Columns:=Array(1, 2, 3, 5), _
                          Header:=xlYes
    End With
    
    ' Process BH 4G sheet - Remove duplicates from columns 1 and 3
    Set ws = ThisWorkbook.Sheets("BH 4G")
    Set tblRange = ws.Range("Tb_Meas_BH_4G[#All]")
    
    With tblRange
        .RemoveDuplicates Columns:=Array(1, 3, 8, 103), _
                          Header:=xlYes
    End With
    
    ' Process Day 4G sheet - Remove duplicates from columns 1 and 3
    Set ws = ThisWorkbook.Sheets("Day 4G")
    Set tblRange = ws.Range("Tb_Meas_Day_4G[#All]")
    
    With tblRange
        .RemoveDuplicates Columns:=Array(1, 3, 8, 103), _
                          Header:=xlYes
    End With
    
    ' Process Day 2G sheet - Remove duplicates from columns 1 and 3
    Set ws = ThisWorkbook.Sheets("Day 2G")
    Set tblRange = ws.Range("Tb_Meas_Day_2G[#All]")
    
    With tblRange
        .RemoveDuplicates Columns:=Array(1, 3, 4, 23), _
                          Header:=xlYes
    End With
    
    ' Restore application settings
    Application.ScreenUpdating = True
    Application.Calculation = xlCalculationAutomatic
    
    Exit Sub
    
ErrorHandler:
    ' Restore application settings in case of error
    Application.ScreenUpdating = True
    Application.Calculation = xlCalculationAutomatic
    
    Debug.Print "Error in RemoveDuplicatesFromTables: " & Err.Description
End Sub

