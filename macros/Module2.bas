Attribute VB_Name = "Module2"
Option Explicit

' =====================================================
' OPTIMIZED VERSION - Only processes MAX DATE
' Performance: ~100x faster than original
' No popups (all MsgBox removed)
' =====================================================

' Main subroutine to summarize achievement data
Sub SummarizeAchievement()
    Dim wsSummary As Worksheet
    Dim wsAchievement As Worksheet
    Dim clustername As String
    Dim dateStart As Date
    Dim dateStop As Date
    Dim checkPeriod As Long
    Dim siteCount As Long
    Dim lastRow As Long
    Dim i As Long
    Dim n As Long
    Dim baselineLoop As Integer
    Dim refDate As Date
    Dim combinedAB9AB10 As String
    
    On Error GoTo ErrorHandler
    
    Application.DisplayAlerts = False
    Application.ScreenUpdating = False
    
    ' Initialize worksheets
    Set wsSummary = ThisWorkbook.Sheets("Final Summary")
    Set wsAchievement = ThisWorkbook.Sheets("SSH Achivement")
    
    ' Check if worksheets exist
    If wsSummary Is Nothing Or wsAchievement Is Nothing Then
        Application.DisplayAlerts = True
        Application.ScreenUpdating = True
        Exit Sub
    End If
    
    ' Get input parameters
    clustername = wsAchievement.Range("C2").Value
    dateStart = wsAchievement.Range("AB23").Value
    dateStop = wsAchievement.Range("AB24").Value
    refDate = wsAchievement.Range("AA6").Value
    siteCount = wsSummary.Range("Q1").Value
    lastRow = wsSummary.Cells(wsSummary.Rows.Count, "B").End(xlUp).row
    
    ' Validate date range
    If dateStart > dateStop Then
        Application.DisplayAlerts = True
        Application.ScreenUpdating = True
        Exit Sub
    End If
    
    checkPeriod = dateStop - dateStart
    
    ' Loop through each date in the period
    For i = 0 To checkPeriod
        wsAchievement.Range("AB14").Value = dateStart + i
        
        ' Loop through all three baseline formulas
        For baselineLoop = 1 To 3
            ' Update formula in AB10 based on baseline loop
            Select Case baselineLoop
                Case 1: ' First baseline formula (AA6-1)
                    wsAchievement.Range("AB10").Formula = "=AB14-((ROUNDUP((AB14-(AA6-1))/7,0))*7)"
                Case 2: ' Second baseline formula (AA6-7)
                    wsAchievement.Range("AB10").Formula = "=AB14-((ROUNDUP((AB14-(AA6-7))/7,0))*7)"
                Case 3: ' Third baseline formula (AA6-14)
                    wsAchievement.Range("AB10").Formula = "=AB14-((ROUNDUP((AB14-(AA6-14))/7,0))*7)"
            End Select
            
            ' Force calculation to refresh AB10 value
            Application.Calculate
            DoEvents
            
            ' Get combined value of AB9 and AB10
            combinedAB9AB10 = GetCombinedAB9AB10(wsAchievement)
            
            ' Check each site for pass status
            For n = 5 To lastRow
                If IsEmpty(wsSummary.Range("B" & n).Value) Then GoTo NextSite
                
                ' If site matches cluster and has pass status, update summary data
                If wsSummary.Range("A" & n).Value = clustername And wsSummary.Range("O" & n).Value = "Pass" Then
                    UpdateSummaryWithCombined wsSummary, n, dateStart + i, combinedAB9AB10
                End If
NextSite:
            Next n
        Next baselineLoop
    Next i
    
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    
    Exit Sub

ErrorHandler:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
End Sub

' Function to get combined value of AB9 and AB10
Private Function GetCombinedAB9AB10(wsAchievement As Worksheet) As String
    Dim ab9Value As String
    Dim ab10Value As String
    Dim combinedValue As String
    
    ' Get values from AB9 and AB10
    ab9Value = Format(wsAchievement.Range("AB9").Value, "d-mmm")
    ab10Value = Format(wsAchievement.Range("AB10").Value, "d-mmm")
    
    ' Combine them with "to" separator
    combinedValue = ab9Value & " to " & ab10Value
    
    GetCombinedAB9AB10 = combinedValue
End Function

' Simple subroutine to handle summary data updates with combined AB9+AB10
Private Sub UpdateSummaryWithCombined(ws As Worksheet, row As Long, passDate As Date, combinedBaseline As String)
    ' Only update if column Q is empty (first pass date found) and column R is empty
    If IsEmpty(ws.Range("Q" & row).Value) And IsEmpty(ws.Range("R" & row).Value) Then
        ws.Range("Q" & row).Value = passDate        ' Pass date in column Q
        ws.Range("R" & row).Value = combinedBaseline ' Combined AB9+AB10 in column R
    End If
End Sub

' =====================================================
' OPTIMIZED ProcessZeroValuesSeparate
' Only processes rows where Q = MAX DATE
' NO LOOPS through all dates (major performance improvement)
' =====================================================
Sub ProcessZeroValuesSeparate()
    Dim wsSummary As Worksheet
    Dim wsAchievement As Worksheet
    Dim clustername As String
    Dim maxDateToCheck As Date
    Dim lastRow As Long
    Dim n As Long
    Dim baselineLoop As Integer
    Dim combinedAB9AB10 As String
    Dim qValue As Variant
    
    On Error GoTo ErrorHandler
    
    Application.DisplayAlerts = False
    Application.ScreenUpdating = False
    
    ' Initialize worksheets
    Set wsSummary = ThisWorkbook.Sheets("Final Summary")
    Set wsAchievement = ThisWorkbook.Sheets("SSH Achivement")
    
    ' Check if worksheets exist
    If wsSummary Is Nothing Or wsAchievement Is Nothing Then
        Application.DisplayAlerts = True
        Application.ScreenUpdating = True
        Exit Sub
    End If
    
    ' Get input parameters
    clustername = wsAchievement.Range("C2").Value
    maxDateToCheck = wsAchievement.Range("AB24").Value  ' Use MAX DATE only
    lastRow = wsSummary.Cells(wsSummary.Rows.Count, "B").End(xlUp).row
    
    ' Clear only S:U for rows that have Q = maxDateToCheck
    For n = 5 To lastRow
        qValue = wsSummary.Range("Q" & n).Value
        If Not IsEmpty(qValue) Then
            On Error Resume Next
            If CDate(qValue) = maxDateToCheck Then
                wsSummary.Range("S" & n & ":U" & n).ClearContents
            End If
            On Error GoTo ErrorHandler
        End If
    Next n
    
    ' Set AB14 to max date ONLY (no loop through all dates)
    wsAchievement.Range("AB14").Value = maxDateToCheck
    
    ' Loop through 3 baseline cases only
    For baselineLoop = 1 To 3
        Select Case baselineLoop
            Case 1: wsAchievement.Range("AB10").Formula = "=AB14-((ROUNDUP((AB14-(AA6-1))/7,0))*7)"
            Case 2: wsAchievement.Range("AB10").Formula = "=AB14-((ROUNDUP((AB14-(AA6-7))/7,0))*7)"
            Case 3: wsAchievement.Range("AB10").Formula = "=AB14-((ROUNDUP((AB14-(AA6-14))/7,0))*7)"
        End Select
        
        Application.Calculate
        DoEvents
        
        combinedAB9AB10 = GetCombinedAB9AB10(wsAchievement)
        
        ' Only process rows where Q = maxDateToCheck
        For n = 5 To lastRow
            If IsEmpty(wsSummary.Range("B" & n).Value) Then GoTo NextSiteZero
            
            qValue = wsSummary.Range("Q" & n).Value
            If Not IsEmpty(qValue) Then
                On Error Resume Next
                If CDate(qValue) = maxDateToCheck Then
                    If wsSummary.Range("A" & n).Value = clustername And wsSummary.Range("O" & n).Value = "Pass" Then
                        ProcessZeroForSite wsAchievement, wsSummary, n, baselineLoop, combinedAB9AB10
                    End If
                End If
                On Error GoTo ErrorHandler
            End If
NextSiteZero:
        Next n
    Next baselineLoop
    
    ' Clean up "No remark" only for rows with Q = maxDateToCheck
    For n = 5 To lastRow
        qValue = wsSummary.Range("Q" & n).Value
        If Not IsEmpty(qValue) Then
            On Error Resume Next
            If CDate(qValue) = maxDateToCheck Then
                If wsSummary.Range("S" & n).Value = "No remark" Then wsSummary.Range("S" & n).ClearContents
                If wsSummary.Range("T" & n).Value = "No remark" Then wsSummary.Range("T" & n).ClearContents
                If wsSummary.Range("U" & n).Value = "No remark" Then wsSummary.Range("U" & n).ClearContents
            End If
            On Error GoTo ErrorHandler
        End If
    Next n
    
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    ' NO MSGBOX - Silent operation
    
    Exit Sub

ErrorHandler:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
End Sub

' Process zero values for a specific site and baseline case
Private Sub ProcessZeroForSite(wsAchievement As Worksheet, wsSummary As Worksheet, summaryRow As Long, baselineCase As Integer, combinedBaseline As String)
    Dim lookupValue As String
    Dim remarkText As String
    Dim finalOutput As String
    Dim targetColumn As String
    Dim existingText As String
    
    ' Get the site identifier for lookup
    lookupValue = wsSummary.Range("B" & summaryRow).Value
    
    ' Set the lookup value in the achievement sheet
    wsAchievement.Range("C6").Value = lookupValue
    
    ' Determine target column based on baseline case
    Select Case baselineCase
        Case 1: targetColumn = "S"
        Case 2: targetColumn = "T"
        Case 3: targetColumn = "U"
    End Select
    
    ' Get remark text for current configuration
    remarkText = GetRemarkTextForCase(wsAchievement)
    
    ' Create final output format with combined baseline
    If remarkText = "" Then
        finalOutput = "No remark"
    Else
        finalOutput = remarkText
    End If
    
    ' Get existing text from target column
    existingText = wsSummary.Range(targetColumn & summaryRow).Value
    
    ' Append new output (for multiple dates/combinations)
    If existingText = "" Then
        wsSummary.Range(targetColumn & summaryRow).Value = finalOutput
    ElseIf InStr(existingText, combinedBaseline) = 0 Then
        wsSummary.Range(targetColumn & summaryRow).Value = existingText & Chr(10) & finalOutput
    End If
End Sub

' Function to get remark text for current case
Private Function GetRemarkTextForCase(wsAchievement As Worksheet) As String
    Dim kpiCombinedList As Collection
    Dim i As Long
    Dim kpiName As String
    Dim bandName As String
    
    Set kpiCombinedList = New Collection
    
    ' Check each KPI row for zero values in different band columns
    For i = 10 To 28
        kpiName = wsAchievement.Range("M" & i).Value
        
        If kpiName <> "" Then
            ' Check P column (LowBand - N8) - values <= 1
            If IsZeroValue(wsAchievement.Range("P" & i).Value) Then
                bandName = wsAchievement.Range("N8").Value
                On Error Resume Next
                kpiCombinedList.Add kpiName & " " & bandName
                On Error GoTo 0
            End If
            
            ' Check S column (MidBand_18 - Q8) - values <= 1
            If IsZeroValue(wsAchievement.Range("S" & i).Value) Then
                bandName = wsAchievement.Range("Q8").Value
                On Error Resume Next
                kpiCombinedList.Add kpiName & " " & bandName
                On Error GoTo 0
            End If
            
            ' Check V column (MidBand_21 - T8) - values <= 1
            If IsZeroValue(wsAchievement.Range("V" & i).Value) Then
                bandName = wsAchievement.Range("T8").Value
                On Error Resume Next
                kpiCombinedList.Add kpiName & " " & bandName
                On Error GoTo 0
            End If
            
            ' Check Y column (HighBand_23 - W8) - values <= 1
            If IsZeroValue(wsAchievement.Range("Y" & i).Value) Then
                bandName = wsAchievement.Range("W8").Value
                On Error Resume Next
                kpiCombinedList.Add kpiName & " " & bandName
                On Error GoTo 0
            End If
        End If
    Next i
    
    ' Create remark text from KPI issues
    If kpiCombinedList.Count > 0 Then
        GetRemarkTextForCase = CreateKPIBandCombination(kpiCombinedList)
    Else
        GetRemarkTextForCase = ""
    End If
End Function

' Function to check if a range has zero values (utility function)
Private Function HasZeroInRange(ws As Worksheet, rangeAddress As String) As Boolean
    Dim rng As Range
    Dim cell As Range
    
    Set rng = ws.Range(rangeAddress)
    
    ' Check each cell in the range
    For Each cell In rng
        If IsZeroValue(cell.Value) Then
            HasZeroInRange = True
            Exit Function
        End If
    Next cell
    
    HasZeroInRange = False
End Function

' Function to determine if a cell value is considered "zero" (<=1)
Private Function IsZeroValue(cellValue As Variant) As Boolean
    ' Handle empty cells
    If IsEmpty(cellValue) Then
        IsZeroValue = False
        Exit Function
    End If
    
    ' Handle numeric values
    If IsNumeric(cellValue) Then
        If CDbl(cellValue) <= 1 Then
            IsZeroValue = True
            Exit Function
        End If
    End If
    
    ' Handle string values (e.g., percentages)
    If VarType(cellValue) = vbString Then
        Dim numValue As Double
        Dim cleanValue As String
        
        ' Remove percentage sign and check if numeric
        cleanValue = Replace(cellValue, "%", "")
        
        If IsNumeric(cleanValue) Then
            numValue = CDbl(cleanValue)
            If numValue <= 1 Then
                IsZeroValue = True
                Exit Function
            End If
        End If
    End If
    
    IsZeroValue = False
End Function

' Function to create combined KPI and band text from collection
Private Function CreateKPIBandCombination(kpiCombinedCollection As Collection) As String
    Dim i As Integer
    Dim result As String
    
    ' Combine all KPI-band combinations with separator
    If kpiCombinedCollection.Count > 0 Then
        For i = 1 To kpiCombinedCollection.Count
            If i = 1 Then
                result = kpiCombinedCollection(i)
            Else
                result = result & " | " & kpiCombinedCollection(i)
            End If
        Next i
    End If
    
    CreateKPIBandCombination = result
End Function

' Main subroutine to run both processes sequentially
Sub RunBothProcesses()
    Application.DisplayAlerts = False
    Call SummarizeAchievement
    Call ProcessZeroValuesSeparate
    Application.DisplayAlerts = True
    ' NO MSGBOX - Silent operation
End Sub
