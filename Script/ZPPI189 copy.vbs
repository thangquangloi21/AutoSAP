Dim SapGuiAuto
Dim application
Dim connection
Dim session
Dim WshShell
Dim i

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

Set application = SapGuiAuto.GetScriptingEngine

'==============================
' MO ENV
'==============================
' application.OpenConnection "2.APOLLON QAS AA3", True
application.OpenConnection "1.APOLLON PRD AA0 Group", True
For i = 1 To 30
    If application.Children.Count > 0 Then Exit For
    WScript.Sleep 1000
Next

Set connection = application.Children(0)

' Doi session khoi tao xong
For i = 1 To 30
    If connection.Sessions.Count > 0 Then Exit For
    WScript.Sleep 1000
Next

'==============================
' DANG NHAP (SSO)
'==============================
WScript.Sleep 2000

' Refresh session
Set session = connection.Sessions(0)

' Kiem tra co man hinh dang nhap khong (chi ton tai tren login screen)
Dim bOnLoginScreen
On Error Resume Next
session.findById("wnd[0]/usr/txtRSYST-MANDT").setFocus
bOnLoginScreen = (Err.Number = 0)
Err.Clear
On Error GoTo 0

If bOnLoginScreen Then
    ' Kiem tra co bang SSO User Selection khong
    Dim oSSOTable
    On Error Resume Next
    Set oSSOTable = session.findById("wnd[0]/usr/tblSAPLSSO2TC_USR_SEL")
    Dim bHasSSO
    bHasSSO = (Err.Number = 0)
    Err.Clear
    On Error GoTo 0

    If bHasSSO Then
        ' Chon dong co Client = 280 trong bang SSO
        Dim nRow, sClient
        For nRow = 0 To oSSOTable.RowCount - 1
            On Error Resume Next
            sClient = session.findById("wnd[0]/usr/tblSAPLSSO2TC_USR_SEL/txtRSYST-MANDT[0," & nRow & "]").Text
            If Err.Number = 0 And Trim(sClient) = "280" Then
                On Error GoTo 0
                oSSOTable.selectedRows = CStr(nRow)
                session.findById("wnd[0]").sendVKey 0
                Exit For
            End If
            On Error GoTo 0
        Next
    Else
        ' Man hinh dang nhap thuong - nhap Client va User
        session.findById("wnd[0]/usr/txtRSYST-MANDT").Text = "280"
        session.findById("wnd[0]/usr/txtRSYST-BNAME").Text = "249533"
        session.findById("wnd[0]").sendVKey 0
    End If

    ' Xu ly popup Multiple Logon neu xuat hien
    WScript.Sleep 2000
    On Error Resume Next
    session.findById("wnd[1]/usr/radMULTI_LOGON_OPT2").select
    If Err.Number = 0 Then
        session.findById("wnd[1]/tbar[0]/btn[0]").press
        WScript.Sleep 1000
    End If
    On Error GoTo 0

    ' Doi SAP Easy Access menu load xong
    For i = 1 To 60
        On Error Resume Next
        Set session = connection.Sessions(0)
        Dim sOKCod
        sOKCod = session.findById("wnd[0]/tbar[0]/okcd").Text
        If Err.Number = 0 Then
            On Error GoTo 0
            Exit For
        End If
        On Error GoTo 0
        WScript.Sleep 1000
    Next
End If

Set session = connection.Sessions(0)

'==============================
' MB52
'==============================
session.findById("wnd[0]").maximize
session.findById("wnd[0]/tbar[0]/okcd").Text = "/nzppi189"
session.findById("wnd[0]").sendVKey 0

' Doi MB52 transaction load xong (toi da 30 giay)
For i = 1 To 30
    On Error Resume Next
    session.findById("wnd[0]/tbar[1]/btn[8]").setFocus
    If Err.Number = 0 Then
        On Error GoTo 0
        Exit For
    End If
    On Error GoTo 0
    WScript.Sleep 1000
Next
session.findById("wnd[0]/tbar[1]/btn[17]").press
session.findById("wnd[1]/usr/cntlALV_CONTAINER_1/shellcont/shell").setCurrentCell 1,"TEXT"
session.findById("wnd[1]/usr/cntlALV_CONTAINER_1/shellcont/shell").selectedRows = "1"
session.findById("wnd[1]/usr/cntlALV_CONTAINER_1/shellcont/shell").doubleClickCurrentCell
session.findById("wnd[0]/tbar[1]/btn[8]").press
session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").setCurrentCell -1,"STATUS"
session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectColumn "STATUS"
session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").contextMenu
session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectContextMenuItem "&XXL"
session.findById("wnd[1]/tbar[0]/btn[0]").press
session.findById("wnd[1]/usr/ctxtDY_PATH").text = "D:\4.DEV\Python\INT189\Data"
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



