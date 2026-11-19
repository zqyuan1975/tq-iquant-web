; 创懿基金量化投资平台 —— Inno Setup 6 安装脚本
; 构建：dist\tq-iquant（Nuitka onedir 产物），先运行 main\build_package.py
; 编译："C:\Users\<用户名>\AppData\Local\Programs\Inno Setup 6\ISCC.exe" packaging\installer.iss

#define AppName "创懿基金量化投资平台"
; 版本号唯一来源：packaging\app-version.iss（build_package.py 也从这里读）
#include "app-version.iss"
; 包名带构建时间戳，便于区分同名不同批次的安装包
#define BuildStamp GetDateTimeString('yyyymmdd-hhnn', '', '')
#define AppPublisher "创懿私募基金"
#define AppExe "tq-iquant.exe"
#define DistDir "..\dist\tq-iquant"

[Setup]
; AppId 固定不变：升级安装时沿用同一卸载项，不会出现两个「程序和功能」条目
AppId={{3B0F4A52-9C61-4C7A-8E1D-2F5B7A9C0D31}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName=D:\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
; 安装到 D:\ 根目录需要管理员权限；程序日常运行不提权
PrivilegesRequired=admin
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
OutputDir=..\dist\installer
OutputBaseFilename={#AppName}-{#AppVersion}-{#BuildStamp}-安装程序
SetupIconFile=icon\app.ico
UninstallDisplayIcon={app}\{#AppExe}
UninstallDisplayName={#AppName}
Compression=lzma2/max
SolidCompression=yes
WizardStyle=modern
; 升级安装时若程序在运行，提示关闭
CloseApplications=yes
RestartApplications=no
ShowLanguageDialog=no

[Languages]
; Inno Setup 官方安装包不自带简体中文，语言文件随仓库放在 packaging\lang\（来自官方 issrc 仓库）
Name: "chinesesimplified"; MessagesFile: "lang\ChineseSimplified.isl"

[Tasks]
Name: "desktopicon"; Description: "创建桌面快捷方式"; GroupDescription: "附加任务:"
; 登录自启：用计划任务在用户登录时拉起 `tq-iquant.exe --supervise`（自带自愈父进程：
; 探活 /health、子进程崩溃退避重启）。不用 Windows 服务——服务在会话 0、没有交互桌面，
; 通达信/同花顺的键鼠自动化会失效。
Name: "autostart"; Description: "登录后自动启动并保持运行（内建自愈守护，推荐）"; GroupDescription: "附加任务:"

[Dirs]
Name: "{app}\data"
Name: "{app}\logs"
Name: "{app}\bridge"

[InstallDelete]
; 覆盖安装时清掉上一版内部模块/前端/迁移的残留文件。
; 目的：避免旧 hash chunk、旧模块文件与新版本混用（旧 exe 复用旧 _internal 的典型故障）。
; 只清程序自身产物，data/、logs/、config.yaml、bridge/ 一律不动。
Type: filesandordirs; Name: "{app}\_internal"
Type: filesandordirs; Name: "{app}\alembic"

[Files]
; 主程序与运行期资源（data/、logs/ 内的用户数据不在本清单内，升级不覆盖）
Source: "{#DistDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
; 桥脚本：必须在 QMT 客户端 Python 3.6.8 / 掘金终端内明文运行，只作为独立文件随包附带
; 只随包 QMT(8790 股票账户) 与 掘金(7801) 两个生产桥；信用账户腿 iquant_bridge_live.py 保留在仓库但不部署
Source: "..\live\bridge\iquant_bridge.py"; DestDir: "{app}\bridge"; Flags: ignoreversion
Source: "..\live\bridge\goldminer_bridge.py"; DestDir: "{app}\bridge"; Flags: ignoreversion
Source: "bridge-deploy.txt"; DestDir: "{app}\bridge"; Flags: ignoreversion

[Icons]
Name: "{group}\{#AppName}"; Filename: "{app}\{#AppExe}"
Name: "{group}\卸载 {#AppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExe}"; Tasks: desktopicon

[Run]
; runasoriginaluser：以启动安装程序的原用户身份（非提权管理员）首次启动，避免把 data/、config.yaml
; 写进管理员拥有的安装目录，导致后续普通用户运行时无写权限。
Filename: "{app}\{#AppExe}"; Description: "立即启动 {#AppName}"; Flags: nowait postinstall skipifsilent runasoriginaluser

[Code]
const
  { 登录自启计划任务名（安装/升级/卸载都按这个名字增删） }
  SupervisorTaskName = 'TQ-IQuant-Supervisor';

var
  RemoveData: Boolean;

function LocalAppDataDir(): String;
begin
  Result := ExpandConstant('{localappdata}\{#AppName}');
end;

procedure ExecLogged(const FileName, Params: String);
var
  ResultCode: Integer;
begin
  if Exec(FileName, Params, '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
    Log('执行: ' + FileName + ' ' + Params + ' -> 退出码 ' + IntToStr(ResultCode))
  else
    Log('执行失败: ' + FileName + ' ' + Params);
end;

{ 创建登录自启任务：登录时以普通权限启动 `tq-iquant.exe --supervise`。
  必须 /RL LIMITED：自愈后端要驱动通达信/同花顺 UI，提权会触发 UIPI 键鼠拦截。 }
procedure CreateSupervisorTask();
begin
  ExecLogged(ExpandConstant('{sys}\schtasks.exe'),
    '/Create /TN "' + SupervisorTaskName + '" /TR "\"' +
    ExpandConstant('{app}\{#AppExe}') + '\" --supervise" /SC ONLOGON /RL LIMITED /F');
end;

procedure DeleteSupervisorTask();
begin
  ExecLogged(ExpandConstant('{sys}\schtasks.exe'),
    '/Delete /TN "' + SupervisorTaskName + '" /F');
end;

{ 结束当前正在运行的该计划任务实例（只杀监督进程；后端子进程由父进程看门狗
  在 5 秒内自行退出，不会留下占端口的孤儿）。 }
procedure StopSupervisorTask();
begin
  ExecLogged(ExpandConstant('{sys}\schtasks.exe'),
    '/End /TN "' + SupervisorTaskName + '"');
end;

{ 是否有本程序进程在跑（tasklist 过滤镜像名；无匹配时输出不含镜像名） }
function AppProcessesRunning(): Boolean;
var
  ProbeFile: String;
  // Inno Setup 6 的 LoadStringFromFile 第二参数签名是 AnsiString，传 String 会 Type mismatch
  Content: AnsiString;
  ResultCode: Integer;
begin
  Result := False;
  ProbeFile := ExpandConstant('{tmp}\tq-iquant-probe.txt');
  DeleteFile(ProbeFile);
  Exec(ExpandConstant('{cmd}'),
    '/c tasklist /FI "IMAGENAME eq {#AppExe}" /NH > "' + ProbeFile + '"',
    '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  if FileExists(ProbeFile) then
  begin
    if LoadStringFromFile(ProbeFile, Content) then
      Result := Pos(Lowercase('{#AppExe}'), Lowercase(Content)) > 0;
    DeleteFile(ProbeFile);
  end;
end;

{ 升级安装：已有 data 目录时提示会保留（不删除、不覆盖） }
function PrepareToInstall(var NeedsRestart: Boolean): String;
begin
  Result := '';
  if DirExists(ExpandConstant('{app}\data')) then
    Log('检测到已有数据目录，升级安装将保留用户数据');
  { 先停自愈监督进程：旧 exe 被占用会导致文件替换失败；等子进程随父退出释放端口 }
  if AppProcessesRunning() then
  begin
    StopSupervisorTask();
    Sleep(6000);
  end;
end;

{ 卸载时询问是否删除数据目录；默认保留 }
function InitializeUninstall(): Boolean;
var
  Msg: String;
begin
  Result := True;
  RemoveData := False;
  Msg := '是否同时删除数据目录（数据库、配置、日志、交割文件）？' + #13#10#13#10 +
         '选择「否」将保留数据，便于将来重新安装后继续使用。';
  if DirExists(ExpandConstant('{app}\data')) or DirExists(LocalAppDataDir()) then
  begin
    if MsgBox(Msg, mbConfirmation, MB_YESNO or MB_DEFBUTTON2) = IDYES then
      RemoveData := True;
  end;
end;

{ 安装收尾：按向导勾选创建/删除登录自启任务 }
procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    if WizardIsTaskSelected('autostart') then
      CreateSupervisorTask()
    else
      DeleteSupervisorTask();
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usUninstall then
  begin
    { 卸载前先停监督进程，避免 exe 被占用导致文件删不掉 }
    if AppProcessesRunning() then
    begin
      StopSupervisorTask();
      Sleep(6000);
    end;
  end
  else if CurUninstallStep = usPostUninstall then
  begin
    DeleteSupervisorTask();
    if RemoveData then
    begin
      DelTree(ExpandConstant('{app}\data'), True, True, True);
      DelTree(ExpandConstant('{app}\logs'), True, True, True);
      DelTree(ExpandConstant('{app}\config.yaml'), True, True, True);
      { 安装目录不可写时程序会回落到 %LOCALAPPDATA%\{#AppName} 存数据 }
      DelTree(LocalAppDataDir(), True, True, True);
    end;
  end;
end;
