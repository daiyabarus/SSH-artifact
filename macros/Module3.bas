Attribute VB_Name = "Module3"
Option Explicit

Sub Save_Range_As_PNG_And_Export_XLSX()
    
    Dim ws_achievement As Worksheet
    Dim ws_hygiene As Worksheet
    Dim ws_bh4g As Worksheet
    Dim ws_final_summary As Worksheet
    Dim newWb As Workbook
    Dim newWs As Worksheet
    Dim newJustificationWs As Worksheet
    Dim clustername As String
    Dim c6Value As String
    Dim ab10Value As String
    Dim ab14Value As String
    Dim sXlsxPath As String
    Dim rng As Range
    Dim bh4gRange As Range
    Dim uniqueFileName As String
    Dim tempChart As ChartObject
    Dim i As Long
    Dim lastRow As Long
    Dim maxDate As Date
    Dim filterValue As String
    Static counter As Long
    
    ' Close any potentially open file handles
    Dim openFiles As Integer
    For openFiles = 1 To 255
        On Error Resume Next
        Close #openFiles
        On Error GoTo 0
    Next openFiles
    
    ' ============ CLEAR FILTERS FIRST ============
    On Error Resume Next
    ' Clear filters on BH 4G sheet using the table structure
    Set ws_bh4g = ThisWorkbook.Sheets("BH 4G")
    If Not ws_bh4g Is Nothing Then
        ws_bh4g.Activate
        ' Select the table header and toggle AutoFilter to clear any existing filters
        ws_bh4g.Range("Tb_Meas_BH_4G[[#Headers],[(4G eNodeB FDD)MSC]]").Select
        Selection.AutoFilter
    End If
    On Error Resume Next
    ' =============================================
    
    On Error Resume Next
    Set ws_achievement = ThisWorkbook.Sheets("SSH Achivement")
    Set ws_hygiene = ThisWorkbook.Sheets("Hygiene Summary")
    Set ws_bh4g = ThisWorkbook.Sheets("BH 4G")
    Set ws_final_summary = ThisWorkbook.Sheets("Final Summary")
    
    If ws_achievement Is Nothing Then
        Exit Sub
    End If
    If ws_hygiene Is Nothing Then
        Exit Sub
    End If
    If ws_bh4g Is Nothing Then
        Exit Sub
    End If
    If ws_final_summary Is Nothing Then
        Exit Sub
    End If
    
    If ActiveWorkbook.Path = "" Then
        Exit Sub
    End If
    
    ' Validate that required cells contain data
    If IsEmpty(ws_achievement.Range("C2")) Or ws_achievement.Range("C2").Value = "" Then
        Exit Sub
    End If
    If IsEmpty(ws_achievement.Range("C6")) Or ws_achievement.Range("C6").Value = "" Then
        Exit Sub
    End If
    
    clustername = CleanFileName(ws_achievement.Range("C2").Value)
    c6Value = CleanFileName(ws_achievement.Range("C6").Value)
    
    ' Handle date formatting safely
    On Error Resume Next
    ab10Value = Format(ws_achievement.Range("AB10").Value, "yyyymmdd")
    If Err.Number <> 0 Then ab10Value = Format(Date, "yyyymmdd")
    ab14Value = Format(ws_achievement.Range("AB14").Value, "yyyymmdd")
    If Err.Number <> 0 Then ab14Value = Format(Date, "yyyymmdd")
    On Error GoTo 0
    
    sXlsxPath = ActiveWorkbook.Path & "\" & clustername & " Report\"
    
    On Error GoTo FolderError
    If Dir(sXlsxPath, vbDirectory) = "" Then
        MkDir sXlsxPath
    End If
    On Error Resume Next
    
    counter = counter + 1
    uniqueFileName = "SSH Achivement Report_" & clustername & "_" & c6Value & "_" & Format(Now, "yyyymmdd_hhmmss")
    
    Set rng = ws_achievement.Range("A1:AC53")
    Dim justificationRng As Range
    Set justificationRng = ws_achievement.Range("A1:Z33")
    
    ws_achievement.Activate
    rng.Select
    
    ws_achievement.Calculate
    Application.Wait Now + TimeValue("00:00:02")
    
    Application.CutCopyMode = False
    
    On Error GoTo CopyError
    rng.CopyPicture Appearance:=xlScreen, Format:=xlBitmap
    On Error Resume Next
    
    If Application.ClipboardFormats(1) = -1 Then
        GoTo Cleanup
    End If
    
    On Error GoTo XlsxError
    Set newWb = Workbooks.Add
    Application.DisplayAlerts = False
    
    ' ============ ADD JUSTIFICATION SHEET FIRST WITH ORANGE TAB COLOR ============
    Set newJustificationWs = newWb.Sheets.Add(Before:=newWb.Sheets(1))
    newJustificationWs.Name = "Justification"
    
    ' Set tab color to orange
    newJustificationWs.Tab.Color = RGB(255, 165, 0)
    
    With newJustificationWs
        .Range("A1:BH4").Merge
        .Range("A1").Value = "Justification"
        .Range("A1").HorizontalAlignment = xlCenter
        .Range("A1").VerticalAlignment = xlCenter
        .Range("A1").Font.Size = 32
        .Range("A1").Font.Bold = True
        
        With .Range("A1").Interior
            .Color = RGB(146, 208, 80)
            .Pattern = xlSolid
        End With
        
        With .Range("A1:BH4").Borders
            .LineStyle = xlContinuous
            .Weight = xlMedium
            .ColorIndex = xlAutomatic
        End With
    End With
    
    ' Copy and paste the A1:Z33 range to Justification sheet with zoom 200
    ws_achievement.Activate
    justificationRng.Select
    On Error GoTo CopyError
    justificationRng.CopyPicture Appearance:=xlScreen, Format:=xlBitmap
    Application.Wait Now + TimeValue("00:00:02")
    On Error Resume Next
    
    newJustificationWs.Activate
    Dim originalJustificationZoom As Integer
    originalJustificationZoom = ActiveWindow.Zoom
    ActiveWindow.Zoom = 200
    
    newJustificationWs.Range("A6").Select
    On Error GoTo PasteImageError
    newJustificationWs.Paste
    Application.Wait Now + TimeValue("00:00:02")
    
    ActiveWindow.Zoom = originalJustificationZoom
    On Error Resume Next
    ' ==============================================================================
    
    ' Create SSH Achievement sheet (STEP 2)
    Set newWs = newWb.Sheets.Add(After:=newJustificationWs)
    newWs.Name = "SSH Achievement"
    
    With newWs
        .Range("A1:BH4").Merge
        .Range("A1").Value = "SSH Achievement Dashboard"
        .Range("A1").HorizontalAlignment = xlCenter
        .Range("A1").VerticalAlignment = xlCenter
        .Range("A1").Font.Size = 32
        .Range("A1").Font.Bold = True
        
        With .Range("A1").Interior
            .Color = RGB(146, 208, 80)
            .Pattern = xlSolid
        End With
        
        With .Range("A1:BH4").Borders
            .LineStyle = xlContinuous
            .Weight = xlMedium
            .ColorIndex = xlAutomatic
        End With
    End With
    
    ' Paste image to SSH Achievement sheet
    newWs.Range("A6").Select
    On Error GoTo PasteImageError
    ' Copy the picture again since it might have been consumed by the previous paste
    ws_achievement.Activate
    rng.CopyPicture Appearance:=xlScreen, Format:=xlBitmap
    Application.Wait Now + TimeValue("00:00:02")
    newWs.Activate
    newWs.Paste
    Application.Wait Now + TimeValue("00:00:02")
    On Error Resume Next

    filterValue = ws_achievement.Range("C6").Value

    ' ============ IMPROVED FILTER CLEARING PROCESS ============
    ws_bh4g.Activate
    
    ' Clear all existing filters using multiple methods to ensure complete clearing
    On Error Resume Next
    
    ' Method 1: Clear AutoFilter if it exists
    If ws_bh4g.AutoFilterMode Then
        ws_bh4g.AutoFilter.ShowAllData
        ws_bh4g.AutoFilterMode = False
    End If
    
    ' Method 2: Clear any other filter modes
    If ws_bh4g.FilterMode Then
        ws_bh4g.ShowAllData
    End If
    
    ' Method 3: Use table-specific filter clearing
    ws_bh4g.Range("Tb_Meas_BH_4G[[#Headers],[(4G eNodeB FDD)MSC]]").Select
    Selection.AutoFilter
    
    On Error GoTo FilterError
    ' ==========================================================
    
    ' Auto-fit columns and set font size
    ws_bh4g.Range("A:H").EntireColumn.AutoFit
    ws_bh4g.Range("A:H").Font.Size = 14

    lastRow = ws_bh4g.Cells(ws_bh4g.Rows.Count, "A").End(xlUp).row

    ' Apply new filters
    With ws_bh4g.Range("A1:H" & lastRow)
        .AutoFilter
        .AutoFilter Field:=4, Criteria1:=filterValue
    End With

    ' Find max date and apply date filter
    On Error Resume Next
    maxDate = Application.WorksheetFunction.Max(ws_bh4g.Range("A:A"))
    On Error GoTo FilterError
    
    ws_bh4g.AutoFilter.Range.AutoFilter Field:=1, Criteria1:=maxDate

    ' Get filtered range
    lastRow = ws_bh4g.Cells(ws_bh4g.Rows.Count, "A").End(xlUp).row
    Set bh4gRange = ws_bh4g.Range("A1:H" & lastRow).SpecialCells(xlCellTypeVisible)

    bh4gRange.Copy

    Dim pasteRow As Long
    Dim lastUsedRow As Long

    lastUsedRow = newWs.Cells(newWs.Rows.Count, "A").End(xlUp).row
    pasteRow = lastUsedRow + 83

    ' Paste with column width auto and text size 14
    With newWs.Cells(pasteRow, 4)
        .PasteSpecial Paste:=xlPasteAll
        .EntireColumn.AutoFit
        .Font.Size = 14
    End With

    ' Clear filters after copying
    On Error Resume Next
    ws_bh4g.Range("Tb_Meas_BH_4G[[#Headers],[(4G eNodeB FDD)MSC]]").Select
    Selection.AutoFilter
    On Error Resume Next
    
    ' ============ ADD HYGIENE SUMMARY SHEET LAST ============
    ws_hygiene.Copy After:=newWb.Sheets(newWb.Sheets.Count)
    ' ========================================================
        
    Do While newWb.Sheets.Count > 3
        For i = newWb.Sheets.Count To 1 Step -1
            If newWb.Sheets(i).Name <> "SSH Achievement" And newWb.Sheets(i).Name <> "Hygiene Summary" And newWb.Sheets(i).Name <> "Justification" Then
                newWb.Sheets(i).Delete
                Exit For
            End If
        Next i
        If newWb.Sheets.Count <= 3 Then Exit Do
    Loop
    
    newWb.SaveAs fileName:=sXlsxPath & uniqueFileName & ".xlsx", FileFormat:=xlOpenXMLWorkbook
    newWb.Close SaveChanges:=False
    
    ' Success message removed - silent operation

Cleanup:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    
    ' Return to SSH Achievement sheet
    On Error Resume Next
    ThisWorkbook.Sheets("SSH Achivement").Select
    ThisWorkbook.Sheets("SSH Achivement").Range("V36").Select
    
    Exit Sub

FolderError:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    ' Error message removed - silent operation
    Exit Sub

CopyError:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    GoTo Cleanup

PasteImageError:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    GoTo Cleanup

FilterError:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    ' Try to clear any partial filters using the table method
    On Error Resume Next
    ws_bh4g.Range("Tb_Meas_BH_4G[[#Headers],[(4G eNodeB FDD)MSC]]").Select
    Selection.AutoFilter
    GoTo Cleanup

XlsxError:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    If Not newWb Is Nothing Then newWb.Close SaveChanges:=False
    GoTo Cleanup
End Sub

' Enhanced CleanFileName function
Function CleanFileName(fileName As String) As String
    If IsEmpty(fileName) Or fileName = "" Then
        CleanFileName = "Unknown"
        Exit Function
    End If
    
    Dim cleanName As String
    cleanName = CStr(fileName)
    
    ' Remove invalid characters
    cleanName = Replace(cleanName, "/", "_")
    cleanName = Replace(cleanName, "\", "_")
    cleanName = Replace(cleanName, ":", "_")
    cleanName = Replace(cleanName, "*", "_")
    cleanName = Replace(cleanName, "?", "_")
    cleanName = Replace(cleanName, """", "_")
    cleanName = Replace(cleanName, "<", "_")
    cleanName = Replace(cleanName, ">", "_")
    cleanName = Replace(cleanName, "|", "_")
    cleanName = Replace(cleanName, vbCr, "")
    cleanName = Replace(cleanName, vbLf, "")
    cleanName = Replace(cleanName, vbCrLf, "")
    
    ' Remove leading/trailing spaces
    cleanName = Trim(cleanName)
    
    ' Ensure it's not empty after cleaning
    If cleanName = "" Then cleanName = "Unknown"
    
    CleanFileName = cleanName
End Function
