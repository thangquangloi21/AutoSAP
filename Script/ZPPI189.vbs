Dim SapGuiAuto
Dim application
Dim connection
Dim session
Dim WshShell
Dim i

Function WaitForControlById(sessionObj, controlId, timeoutSeconds)
    Dim j
    Dim ctrl
    For j = 1 To timeoutSeconds
        On Error Resume Next
        Set ctrl = sessionObj.findById(controlId)
        If Err.Number = 0 Then
            On Error GoTo 0
            Set WaitForControlById = ctrl
            Exit Function
        End If
        Err.Clear
        On Error GoTo 0
        WScript.Sleep 1000
    Next

    Set WaitForControlById = Nothing
End Function

Set WshShell = CreateObject("WScript.Shell")

'==============================
' MO SAP LOGON
'==============================
WshShell.Run """C:\Program Files (x86)\SAP\FrontEnd\SAPgui\saplogon.exe"""
'==============================
' DOI SAP GUI SAN SANG
'==============================
For i = 1 To 30
    On Error Resume Next
    Set SapGuiAuto = GetObject("SAPGUI")
    If Err.Number = 0 Then
        On Error GoTo 0
        Exit For
    End If
    Err.Clear
    Set SapGuiAuto = Nothing
    On Error GoTo 0
    WScript.Sleep 1000
Next

If SapGuiAuto Is Nothing Then
    MsgBox "Khong bat duoc SAPGUI"
    WScript.Quit
End If

Set SapGuiAuto  = GetObject("SAPGUI")
Set application = SapGuiAuto.GetScriptingEngine

Set connection = application.OpenConnection("1.APOLLON PRD AA0 Group", True)

' Đợi session
Do While connection.Children.Count = 0
    WScript.Sleep 500
Loop

Set session = connection.Children(0)

WScript.Sleep 2000

' LOGIN
session.findById("wnd[0]/usr/txtRSYST-MANDT").Text = "100"
session.findById("wnd[0]/usr/txtRSYST-BNAME").Text = "260825"
session.findById("wnd[0]/usr/pwdRSYST-BCODE").Text = "Terumo9999aaaa#"
session.findById("wnd[0]/usr/txtRSYST-LANGU").Text = "EN"

session.findById("wnd[0]").sendVKey 0

' ===== HANDLE MULTI LOGIN =====
WScript.Sleep 1500

If session.Children.Count > 1 Then
    On Error Resume Next
    
    session.findById("wnd[1]/usr/radMULTI_LOGON_OPT2").Select
    session.findById("wnd[1]/tbar[0]/btn[0]").Press
    
    On Error GoTo 0
End If

Set session = connection.Sessions(0)

'==============================
' MB52
'==============================
session.findById("wnd[0]").maximize
session.findById("wnd[0]/tbar[0]/okcd").Text = "/nzppi189"
session.findById("wnd[0]").sendVKey 0

' Doi MB52 transaction load xong (toi da 30 giay)
Dim toolbarButton
Set toolbarButton = WaitForControlById(session, "wnd[0]/tbar[1]/btn[8]", 30)
If toolbarButton Is Nothing Then
    WScript.Echo "Timeout waiting for control: wnd[0]/tbar[1]/btn[8]"
    WScript.Quit 1
End If
toolbarButton.setFocus

Dim exportButton
Set exportButton = WaitForControlById(session, "wnd[0]/tbar[1]/btn[17]", 30)
If exportButton Is Nothing Then
    WScript.Echo "Timeout waiting for control: wnd[0]/tbar[1]/btn[17]"
    WScript.Quit 1
End If
exportButton.press
session.findById("wnd[1]/usr/cntlALV_CONTAINER_1/shellcont/shell").setCurrentCell 1,"TEXT"
session.findById("wnd[1]/usr/cntlALV_CONTAINER_1/shellcont/shell").selectedRows = "1"
session.findById("wnd[1]/usr/cntlALV_CONTAINER_1/shellcont/shell").doubleClickCurrentCell
session.findById("wnd[0]/tbar[1]/btn[8]").press
session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").setCurrentCell -1,"STATUS"
session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectColumn "STATUS"
session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").contextMenu
session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectContextMenuItem "&XXL"
session.findById("wnd[1]/tbar[0]/btn[0]").press
session.findById("wnd[1]/usr/ctxtDY_PATH").text = "D:\4.DEV\Python\AutoSAP\Data"
session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "INT189.XLSX"
session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 6
session.findById("wnd[1]/tbar[0]/btn[11]").press


'==============================
' TAT excel INT189.XLSX
'==============================
WScript.Sleep 2000
Dim objExcel
On Error Resume Next 
Set objExcel = GetObject(, "Excel.Application")
If Err.Number = 0 Then
    objExcel.Workbooks("INT189.XLSX").Close False
    objExcel.Quit
End If  
On Error GoTo 0

'==============================
' TAT SAP
'==============================
WScript.Sleep 2000
session.findById("wnd[0]/tbar[0]/okcd").Text = "/nex"
session.findById("wnd[0]").sendVKey 0



