# --------------------------------------------------------------------------
# Copyright Commvault Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------

from fastmcp.exceptions import ToolError
from typing import Annotated
from pydantic import Field
import boto3

from src.cv_api_client import commvault_api_client
from src.logger import logger
from src.wrappers import format_report_dataset_response
import os
import json


DOCUSIGN_BACKUP_WORKFLOW_XML = """<Workflow_WorkflowDefinition outputs="&lt;outputs/&gt;" webHidden="0" isHtmlDescription="0" inputs="&lt;inputs/&gt;" interactive="0" description="" manualPercentageComplete="0" apiMode="0" executeOnWeb="0" variables="&lt;variables&gt;&#xA;  &lt;exitCode class=&quot;java.lang.Integer&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;message class=&quot;java.lang.String&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;isWindows class=&quot;java.lang.Integer&quot; _list_=&quot;false&quot;&gt;0&lt;/isWindows&gt;&#xA;&lt;/variables&gt;" revision="$Revision:  $" tags="" modTime="1755767897" uniqueGuid="5beacc90-e05c-40b1-af71-d58c5901da15" name="Backup Docusign" config="&lt;configuration&gt;&#xA;  &lt;privateKey class=&quot;java.lang.String&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;configJson class=&quot;java.lang.String&quot; _list_=&quot;false&quot;/&gt;&#xA;&lt;/configuration&gt;" workflowId="0"><schema><outputs><children hidden="0" defaultValue="" className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" listType="0" inputType="java.lang.String" attribute="0" documentation="" readOnly="0" controlType="0" name="exitCode" /><children hidden="0" defaultValue="" className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" listType="0" inputType="java.lang.String" attribute="0" documentation="" readOnly="0" controlType="0" name="message" /></outputs><variables><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" listType="0" inputType="java.lang.Integer" name="exitCode" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" listType="0" inputType="java.lang.String" name="message" /><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" listType="0" inputType="java.lang.Integer" name="isWindows" /></variables><inputs /><config className="" type="" name="configuration"><children displayName="" className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" listType="0" required="0" inputType="java.lang.String" documentation="" controlHidden="0" readOnly="0" searchable="0" controlType="0" name="privateKey" alignment="0" /><children displayName="" className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" listType="0" required="0" inputType="java.lang.String" documentation="" controlHidden="0" readOnly="0" searchable="0" controlType="0" name="configJson" alignment="0" /></config></schema><Start displayName="Start" interactive="0" originalStyle="" jobMode="0" description="" waitSetting="0" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="36" created="0" uniqueName="Start_1" skipAttempt="0" name="Start" width="55" x="45" y="41" style="image;image=/images/jgraphx/house.png"><inputs val="&lt;inputs/&gt;" /><transition activity="AcquireLock_1" points="126,58" value="ANY" /></Start><Activity maxRestarts="0" displayName="Python3 Available?" interactive="0" originalStyle="" jobMode="0" description="Execute a script on a remote machine" waitSetting="0" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="25" created="1755510637092" uniqueName="ExecuteScript_1" skipAttempt="0" name="ExecuteScript" width="85" x="263" y="42" style="label;image=commvault.cte.workflow.activities.ExecuteScript"><inputs val="&lt;inputs&gt;&#xA;  &lt;script&gt;Write-Host &quot;Checking for Python 3 and pip3...&quot;&#xA;&#xA;# Function to check command existence&#xA;function Test-Command($cmd) {&#xA;    $null -ne (Get-Command $cmd -ErrorAction SilentlyContinue)&#xA;}&#xA;&#xA;$pythonOK = $false&#xA;$pipOK    = $false&#xA;&#xA;# ---- Check Python 3 ----&#xA;if (Test-Command &quot;python3&quot;) {&#xA;    $ver = python3 --version 2&amp;gt;&amp;amp;1&#xA;    Write-Host &quot;python3 found: $ver&quot;&#xA;&#xA;    if ($ver -match &quot;Python 3\.\d+&quot;) {&#xA;        $pythonOK = $true&#xA;    } else {&#xA;        Write-Host &quot;Found python3 but version is not 3.x&quot;&#xA;    }&#xA;} elseif (Test-Command &quot;python&quot;) {&#xA;    $ver = python --version 2&amp;gt;&amp;amp;1&#xA;    Write-Host &quot;python found: $ver&quot;&#xA;&#xA;    if ($ver -match &quot;Python 3\.\d+&quot;) {&#xA;        $pythonOK = $true&#xA;    } else {&#xA;        Write-Host &quot;python exists but it&#x2019;s not Python 3&quot;&#xA;    }&#xA;} else {&#xA;    Write-Host &quot;Python 3 is not installed&quot;&#xA;}&#xA;&#xA;# ---- Check pip3 ----&#xA;if (Test-Command &quot;pip3&quot;) {&#xA;    $ver = pip3 --version 2&amp;gt;&amp;amp;1&#xA;    Write-Host &quot;pip3 found: $ver&quot;&#xA;    $pipOK = $true&#xA;} elseif (Test-Command &quot;pip&quot;) {&#xA;    $ver = pip --version 2&amp;gt;&amp;amp;1&#xA;    Write-Host &quot;pip found: $ver&quot;&#xA;    if ($ver -match &quot;python 3\.\d+&quot;) {&#xA;        $pipOK = $true&#xA;    } else {&#xA;        Write-Host &quot;pip exists but tied to non-Python3&quot;&#xA;    }&#xA;} else {&#xA;    Write-Host &quot;pip3 is not installed&quot;&#xA;}&#xA;&#xA;# ---- Exit code ----&#xA;if ($pythonOK -and $pipOK) {&#xA;    Write-Host &quot;Python 3 and pip3 are installed&quot;&#xA;    exit 0&#xA;} else {&#xA;    Write-Host &quot;Python 3 or pip3 missing&quot;&#xA;    exit 1&#xA;}&#xA;&lt;/script&gt;&#xA;  &lt;scriptType&gt;PowerShell&lt;/scriptType&gt;&#xA;  &lt;client/&gt;&#xA;&lt;/inputs&gt;" /><onExit language="3" script="var exitcode = xpath:{/workflow/ExecuteScript_1/exitCode};&#xA;if(exitcode &amp;&amp; exitcode === 0){&#xA;&#x9;workflow.setVariable(&quot;exitCode&quot;, 0);&#xA;}else{&#xA;  &#x9;workflow.setVariable(&quot;exitCode&quot;, 1);&#xA;&#x9;workflow.setVariable(&quot;message&quot;, xpath:{/workflow/ExecuteScript_1/commandOutput});&#xA;&#xA;}&#xA;&#xA;" /><transition sourceX="258" sourceY="69" activity="ReleaseLock_1" displayName="No" targetY="163" targetX="258" originalStyle="" description="" x="0" y="0" transitionIndex="0" style="defaultEdge" value="ANY" commented="0" status="0"><condition language="3" script="/*&#xA;The expression should return a boolean. Use the variable name &quot;activity&quot; to refer to the previous activity object. Example:&#xA;activity.exitCode==0;&#xA;*/&#xA;xpath:{/workflow/ExecuteScript_1/exitCode} == 1;" /></transition><transition activity="ExecuteProcessBlock_1" displayName="Yes" points="468,53" value="ANY"><condition script="xpath:{/workflow/ExecuteScript_1/exitCode} == 0;" /></transition></Activity><Activity displayName="WorkflowEnd" interactive="0" originalStyle="" jobMode="0" description="Ends the workflow" waitSetting="0" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="27" created="1755512827384" uniqueName="WorkflowEnd_1" skipAttempt="0" name="WorkflowEnd" width="108" x="271" y="162" style="label;image=commvault.cte.workflow.activities.EndActivity"><inputs val="&lt;inputs&gt;&#xA;  &lt;completionStatus class=&quot;workflow.types.WorkflowCompletionStatus&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;failureMessage class=&quot;java.lang.String&quot; _list_=&quot;false&quot;/&gt;&#xA;&lt;/inputs&gt;" /><outputs outputs="&lt;outputs&gt;&#xA;  &lt;exitCode&gt;xpath:{/workflow/variables/exitCode}&lt;/exitCode&gt;&#xA;  &lt;message&gt;xpath:{/workflow/variables/message}&lt;/message&gt;&#xA;&lt;/outputs&gt;" /></Activity><Activity displayName="AcquireLock" description="synchronizes a workflow per named parameter" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="23" created="1755597745277" breakpoint="0" uniqueName="AcquireLock_1" name="AcquireLock" width="86" x="135" y="45"><inputs val="&lt;inputs&gt;&#xA;  &lt;name class=&quot;java.lang.String&quot; _list_=&quot;false&quot;&gt;xpath:{/workflow/system/workflow/workflowName}&lt;/name&gt;&#xA;  &lt;releaseLockOnCompletion class=&quot;java.lang.Boolean&quot; _list_=&quot;false&quot;&gt;true&lt;/releaseLockOnCompletion&gt;&#xA;  &lt;timeout class=&quot;java.lang.Integer&quot; _list_=&quot;false&quot;/&gt;&#xA;&lt;/inputs&gt;" /><activitySchema><outputs><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" documentation="the name you want to lock on" name="name" /><children className="java.lang.Boolean" type="{http://www.w3.org/2001/XMLSchema}boolean" inputType="java.lang.Boolean" name="lockAquired" /></outputs><inputs><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" documentation="the name you want to lock on" name="name" /><children defaultValue="true" className="java.lang.Boolean" type="{http://www.w3.org/2001/XMLSchema}boolean" inputType="java.lang.Boolean" documentation="releases the lock automatically when the workflow completes" name="releaseLockOnCompletion" /><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" documentation="timeout in minutes for trying to acquire the lock" name="timeout" /></inputs></activitySchema><transition activity="Script_4" value="ANY" /></Activity><Activity displayName="ReleaseLock" description="releases the lock for the named parameter" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="22" created="1755597814236" breakpoint="0" uniqueName="ReleaseLock_1" name="ReleaseLock" width="88" x="264" y="110"><inputs val="&lt;inputs&gt;&#xA;  &lt;name class=&quot;java.lang.String&quot; _list_=&quot;false&quot;&gt;xpath:{/workflow/system/workflow/workflowName}&lt;/name&gt;&#xA;&lt;/inputs&gt;" /><activitySchema><outputs><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" documentation="the name of the lock you want to release" name="name" /></outputs><inputs><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" documentation="the name of the lock you want to release" name="name" /></inputs></activitySchema><transition activity="WorkflowEnd_1" value="ANY" /></Activity><Activity maxRestarts="0" displayName="Is Windows" description="activity to execute code snippets in the selected language" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="21" created="1756102342592" breakpoint="0" uniqueName="Script_4" name="Script" width="80" x="137" y="96"><inputs val="&lt;inputs&gt;&#xA;  &lt;script language=&quot;4&quot; script=&quot;String os = System.getProperty(&amp;quot;os.name&amp;quot;).toLowerCase();&amp;#xA;int isWindows = 0;&amp;#xA;if (os.contains(&amp;quot;win&amp;quot;)) {&amp;#xA;  logger.info(&amp;quot;Running on Windows&amp;quot;);&amp;#xA;  isWindows = 1;&amp;#xA;} else if (os.contains(&amp;quot;nix&amp;quot;) || os.contains(&amp;quot;nux&amp;quot;) || os.contains(&amp;quot;aix&amp;quot;)) {&amp;#xA;  logger.info(&amp;quot;Running on Linux&amp;quot;);&amp;#xA;} else if (os.contains(&amp;quot;mac&amp;quot;)) {&amp;#xA;  logger.info(&amp;quot;Running on macOS&amp;quot;);&amp;#xA;} else {&amp;#xA;  logger.info(&amp;quot;Running on &amp;quot; + os);&amp;#xA;}&amp;#xA;workflow.setVariable(&amp;quot;isWindows&amp;quot;, isWindows);&amp;#xA;return isWindows;&quot;/&gt;&#xA;&lt;/inputs&gt;" /><activitySchema><outputs><children className="java.lang.Object" type="{http://www.w3.org/2001/XMLSchema}anyType" inputType="java.lang.Object" name="output" /></outputs><inputs /></activitySchema><transition activity="ExecuteScript_1" displayName="Windows" value="ANY"><condition script="xpath:{/workflow/Script_4/output} == true;" /></transition><transition activity="ExecuteScript_4" displayName="Linux" value="ANY" /></Activity><Activity displayName="Python3 Available?" description="Execute a script on a remote machine" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="25" created="1756102643525" breakpoint="0" uniqueName="ExecuteScript_4" name="ExecuteScript" width="114" x="130" y="162"><inputs val="&lt;inputs&gt;&#xA;  &lt;impersonateUserName class=&quot;java.lang.String&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;impersonateUserPassword class=&quot;workflow.types.EncryptedString&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;startUpPath class=&quot;java.lang.String&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;scriptType class=&quot;commvault.msgs.App.ScriptType&quot; _list_=&quot;false&quot;&gt;UnixShell&lt;/scriptType&gt;&#xA;  &lt;script class=&quot;java.lang.String&quot; _list_=&quot;false&quot;&gt;#!/bin/bash&#xA;&#xA;# Check if python3 exists&#xA;if command -v python3 &amp;amp;&amp;gt;/dev/null; then&#xA;    echo &quot;Python 3 is installed: $(python3 --version)&quot;&#xA;    exit 0&#xA;else&#xA;    echo &quot;Python 3 is not installed.&quot;&#xA;    exit 1&#xA;fi&#xA;&lt;/script&gt;&#xA;  &lt;arguments class=&quot;java.lang.String&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;waitForProcessCompletion class=&quot;java.lang.Boolean&quot; _list_=&quot;false&quot;&gt;true&lt;/waitForProcessCompletion&gt;&#xA;  &lt;client&gt;&#xA;    &lt;clientName/&gt;&#xA;    &lt;clientId/&gt;&#xA;    &lt;hostName/&gt;&#xA;    &lt;clientGUID/&gt;&#xA;    &lt;displayName/&gt;&#xA;    &lt;commCellName/&gt;&#xA;    &lt;csGUID/&gt;&#xA;    &lt;type/&gt;&#xA;    &lt;flags/&gt;&#xA;    &lt;GUID/&gt;&#xA;    &lt;newName/&gt;&#xA;  &lt;/client&gt;&#xA;&lt;/inputs&gt;" /><activitySchema><outputs><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" documentation="the exitCode recieved from executing the command" name="exitCode" /><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" documentation="the return code recieved from completion of the command" name="errorCode" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" documentation="the output from the command or error message if it failed" name="commandOutput" /></outputs><inputs type=""><children className="commvault.msgs.CvEntities.ClientEntity" type="{commvault.msgs.CvEntities}ClientEntity" inputType="commvault.msgs.CvEntities.ClientEntity" documentation="the remote host to execute the command on" name="client"><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="clientName" /><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" name="clientId" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="clientName" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="hostName" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="clientGUID" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="displayName" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="commCellName" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="csGUID" /><children className="commvault.msgs.CvEntities.CommCellType" type="{commvault.msgs.CvEntities}CommCellType" inputType="commvault.msgs.CvEntities.CommCellType" name="type"><options val="GALAXY" /><options val="NBU" /><options val="NETAPP" /><options val="TSM" /><options val="VEEAM" /></children><children className="commvault.msgs.CvEntities.EntityFlags" type="{commvault.msgs.CvEntities}EntityFlags" inputType="commvault.msgs.CvEntities.EntityFlags" name="flags" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="GUID" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="newName" /></children><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" required="1" inputType="java.lang.String" documentation="the host username to execute the command as" name="impersonateUserName" /><children className="workflow.types.EncryptedString" type="{workflow.types}EncryptedString" required="1" inputType="workflow.types.EncryptedString" documentation="the credentials to execute the command as" name="impersonateUserPassword" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" required="1" inputType="java.lang.String" documentation="the path to the executable" name="startUpPath" /><children className="commvault.msgs.App.ScriptType" type="{commvault.msgs.App}ScriptType" required="1" inputType="commvault.msgs.App.ScriptType" name="scriptType"><options val="Java" /><options val="Python" /><options val="PowerShell" /><options val="WindowsBatch" /><options val="UnixShell" /><options val="Perl" /></children><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" required="1" inputType="java.lang.String" name="script" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" documentation="arguments to pass into command" name="arguments" /><children defaultValue="true" className="java.lang.Boolean" type="{http://www.w3.org/2001/XMLSchema}boolean" inputType="java.lang.Boolean" documentation="tells the activity to wait for process to complete and retrieve the output from the command" name="waitForProcessCompletion" /></inputs></activitySchema><transition activity="ReleaseLock_1" displayName="No" points="226,149" value="ANY"><condition script="xpath:{/workflow/ExecuteScript_4/exitCode} == 1;" /></transition><transition activity="ExecuteProcessBlock_1" displayName="Yes" points="469,215" value="ANY"><condition script="xpath:{/workflow/ExecuteScript_4/exitCode} == 0;" /></transition></Activity><Activity maxRestarts="0" displayName="Install required lib and run backup" description="creates a super process group" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="185" created="1756103171107" breakpoint="0" uniqueName="ProcessBlock_1" name="ProcessBlock" width="430" x="566" y="62"><inputs val="&lt;inputs&gt;&#xA;  &lt;inputs&gt;&#xA;    &lt;inputs class=&quot;workflow.types.XML&quot; _list_=&quot;false&quot;/&gt;&#xA;    &#xA;  &lt;/inputs&gt;&#xA;&lt;/inputs&gt;" /><superProcess><Start displayName="Start" description="" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="40" created="1756103171111" breakpoint="0" uniqueName="Start_2" skipAttempt="0" name="Start" width="60" x="10" y="37"><inputs val="&lt;inputs/&gt;" /><transition activity="ExecuteScript_2" value="ANY" /></Start><Activity maxRestarts="0" displayName="Install Required Lib" interactive="0" originalStyle="" jobMode="0" description="Execute a script on a remote machine" waitSetting="0" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="25" created="1755510797959" uniqueName="ExecuteScript_2" skipAttempt="0" name="ExecuteScript" width="35" x="83" y="45" style="label;image=commvault.cte.workflow.activities.ExecuteScript"><inputs val="&lt;inputs&gt;&#xA;  &lt;client/&gt;&#xA;  &lt;scriptType&gt;Python&lt;/scriptType&gt;&#xA;  &lt;script&gt;import importlib&#xA;import subprocess&#xA;import sys&#xA;&#xA;# List of required packages&#xA;REQUIRED_LIBS = [&#xA;    &quot;boto3&quot;, &quot;requests&quot;, &quot;PyJWT&quot;, &quot;PyYAML&quot;, &quot;cryptography&quot;&#xA;]&#xA;&#xA;def install_package(package):&#xA;    &quot;&quot;&quot;Install package using pip&quot;&quot;&quot;&#xA;    try:&#xA;        subprocess.check_call([sys.executable, &quot;-m&quot;, &quot;pip&quot;, &quot;install&quot;, package])&#xA;    except subprocess.CalledProcessError as e:&#xA;        print(f&quot;Failed to install {package}: {e}&quot;)&#xA;        sys.exit(1)&#xA;&#xA;def ensure_packages():&#xA;    &quot;&quot;&quot;Ensure all required packages are installed&quot;&quot;&quot;&#xA;    for lib in REQUIRED_LIBS:&#xA;        try:&#xA;            importlib.import_module(lib)&#xA;            print(f&quot;{lib} already installed&quot;)&#xA;        except ImportError:&#xA;            print(f&quot;Installing {lib} ...&quot;)&#xA;            install_package(lib)&#xA;&#xA;&#xA;ensure_packages()&#xA;print(&quot;All required packages are installed.&quot;)&#xA;sys.exit(0)&lt;/script&gt;&#xA;&lt;/inputs&gt;" /><onExit language="3" script="var exitcode = xpath:{/workflow/ExecuteScript_2/exitCode};&#xA;if(exitcode &amp;&amp; exitcode === 0){&#xA;&#x9;workflow.setVariable(&quot;exitCode&quot;, 0);&#xA;}else{&#xA;  &#x9;workflow.setVariable(&quot;exitCode&quot;, 2);&#xA;&#x9;workflow.setVariable(&quot;message&quot;, xpath:{/workflow/ExecuteScript_2/commandOutput});&#xA;&#xA;}&#xA;&#xA;" /><transition activity="Break_1" displayName="Falied" value="ANY"><condition script="xpath:{/workflow/ExecuteScript_2/exitCode} == 1;" /></transition><transition activity="ExecuteScript_3" displayName="All Installed" value="ANY"><condition script="xpath:{/workflow/ExecuteScript_2/exitCode} == 0;" /></transition></Activity><Activity maxRestarts="0" displayName="Run Backup" description="Execute a script on a remote machine" timeout="0" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="17" created="1755527270431" breakpoint="0" uniqueName="ExecuteScript_3" name="ExecuteScript" width="98" x="109" y="115"><inputs val="&lt;inputs&gt;&#xA;  &lt;impersonateUserName class=&quot;java.lang.String&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;impersonateUserPassword class=&quot;workflow.types.EncryptedString&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;startUpPath class=&quot;java.lang.String&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;scriptType class=&quot;commvault.msgs.App.ScriptType&quot; _list_=&quot;false&quot;&gt;Python&lt;/scriptType&gt;&#xA;  &lt;script class=&quot;java.lang.String&quot; _list_=&quot;false&quot;&gt;import importlib&#xA;import boto3&#xA;import logging&#xA;import requests&#xA;import yaml&#xA;import mimetypes&#xA;import os&#xA;from pathlib import Path&#xA;import argparse&#xA;from datetime import datetime, timedelta, UTC&#xA;from botocore.exceptions import ClientError&#xA;from urllib.parse import urlencode&#xA;import jwt&#xA;import tempfile&#xA;import sys&#xA;import json&#xA;&#xA;# Ensure logs directory exists&#xA;isWindows = xpath:{/workflow/variables/isWindows}&#xA;script_directory = os.path.dirname(os.path.abspath(__file__))&#xA;if(isWindows == 1):&#xA;&#x9;log_directory = os.path.join(script_directory, &quot;../../Log Files&quot;)&#xA;else:&#xA;&#x9;log_directory = os.path.join(&quot;/var/log/commvault/&quot;, &quot;Log_Files&quot;)&#xA;print(f&quot;Logs will be saved in {log_directory} folder, docusign_backup.log file.&quot;)&#xA;os.makedirs(log_directory, exist_ok=True)&#xA;&#xA;# Setup logger to file and console&#xA;logging.basicConfig(&#xA;    level=logging.INFO,&#xA;    format=&quot;%(asctime)s [%(levelname)s] %(name)s: %(message)s&quot;,&#xA;    handlers=[&#xA;        logging.FileHandler(os.path.join(log_directory, &quot;docusign_backup.log&quot;), encoding=&quot;utf-8&quot;),&#xA;        logging.StreamHandler()  # also log to console&#xA;    ]&#xA;)&#xA;&#xA;logger = logging.getLogger(&quot;docusign_backup&quot;)&#xA;&#xA;&#xA;required_packages = [&#xA;    &quot;boto3&quot;,&#xA;    &quot;requests&quot;,&#xA;    &quot;jwt&quot;,      # PyJWT&#xA;    &quot;yaml&quot;      # PyYAML&#xA;]&#xA;&#xA;missing = []&#xA;&#xA;for package in required_packages:&#xA;    try:&#xA;        importlib.import_module(package)&#xA;    except ImportError:&#xA;        missing.append(package)&#xA;&#xA;if missing:&#xA;    logger.exception(f&quot;Missing required libraries: {&apos;, &apos;.join(missing)}&quot;)&#xA;    logger.info(&quot;Run: pip install -r requirements.txt&quot;)&#xA;    sys.exit(1)&#xA;&#xA;LAST_RUN_KEY = &apos;docusign-backup/last-run.txt&apos;&#xA;&#xA;&#xA;def load_config(path):&#xA;    with open(path) as f:&#xA;        return yaml.safe_load(f)&#xA;&#xA;def load_config_from_json(json_str):&#xA;    try:&#xA;        return json.loads(json_str)&#xA;    except json.JSONDecodeError as e:&#xA;        logger.exception(f&quot;Invalid JSON input: {e}&quot;)&#xA;&#xA;&#xA;def get_s3_client(config):&#xA;    try:&#xA;        return boto3.client(&#xA;            &apos;s3&apos;,&#xA;            endpoint_url=config[&apos;aws&apos;][&apos;endpoint&apos;],&#xA;            region_name=config[&apos;aws&apos;][&apos;region&apos;],&#xA;            aws_access_key_id=config[&apos;aws&apos;][&apos;accessKeyId&apos;],&#xA;            aws_secret_access_key=config[&apos;aws&apos;][&apos;secretAccessKey&apos;],&#xA;            config=boto3.session.Config(s3={&apos;addressing_style&apos;: &apos;path&apos;})&#xA;        )&#xA;    except KeyError as e:&#xA;        logger.exception(f&quot;Missing AWS config key: {e}. Please check your configuration file.&quot;)&#xA;        raise&#xA;&#xA;def get_docusign_auth_server(docu_config):&#xA;    try:&#xA;        return docu_config[&apos;authServer&apos;]&#xA;    except KeyError:&#xA;        logger.exception(&quot;Missing &apos;authServer&apos; in DocuSign config. Please check your configuration file.&quot;)&#xA;        raise&#xA;&#xA;def get_access_token(config):&#xA;    try:&#xA;        docu_config = config[&apos;docusign&apos;]&#xA;        #with open(docu_config[&apos;privateKeyPath&apos;], &apos;r&apos;) as f:&#xA;        #    private_key = f.read()&#xA;        private_key = r&quot;&quot;&quot;xpath:{/workflow/configuration/privateKey}&quot;&quot;&quot;&#xA;    except Exception as e:&#xA;        logger.exception(f&quot;Failed to read private key from workflow configuration privateKey, exception: {e}&quot;)&#xA;        raise&#xA;    now = int(datetime.now(UTC).timestamp())&#xA;    try:&#xA;        authserver = get_docusign_auth_server(docu_config)&#xA;        payload = {&#xA;            &apos;iss&apos;: docu_config[&apos;integrationKey&apos;],&#xA;            &apos;sub&apos;: docu_config[&apos;userId&apos;],&#xA;            &apos;aud&apos;: authserver,&#xA;            &apos;iat&apos;: now,&#xA;            &apos;exp&apos;: now + 120,&#xA;            &apos;scope&apos;: docu_config[&apos;scopes&apos;]&#xA;        }&#xA;        assertion = jwt.encode(payload, private_key, algorithm=&apos;RS256&apos;)&#xA;        headers = {&apos;Content-Type&apos;: &apos;application/x-www-form-urlencoded&apos;}&#xA;        body = urlencode({&#xA;            &apos;grant_type&apos;: &apos;urn:ietf:params:oauth:grant-type:jwt-bearer&apos;,&#xA;            &apos;assertion&apos;: assertion&#xA;        })&#xA;        &#xA;    except KeyError as e:&#xA;        logger.exception(f&quot;Missing DocuSign config key: {e}. Please check your configuration file.&quot;)&#xA;        raise&#xA;    url = f&quot;https://{authserver}/oauth/token&quot;&#xA;    res = requests.post(url, headers=headers, data=body)&#xA;    res.raise_for_status()&#xA;    return res.json()[&apos;access_token&apos;]&#xA;&#xA;&#xA;def get_user_info(config, token):&#xA;    headers = {&apos;Authorization&apos;: f&apos;Bearer {token}&apos;}&#xA;    res = requests.get(f&quot;https://{get_docusign_auth_server(config[&apos;docusign&apos;])}/oauth/userinfo&quot;, headers=headers)&#xA;    res.raise_for_status()&#xA;    return res.json()[&apos;accounts&apos;][0]&#xA;&#xA;&#xA;def list_completed_envelopes(config, account_id, token, from_date):&#xA;    url = f&quot;{get_base_path(config)}/v2.1/accounts/{account_id}/envelopes?from_date={from_date}&amp;amp;status=completed&quot;&#xA;    headers = {&apos;Authorization&apos;: f&apos;Bearer {token}&apos;}&#xA;    res = requests.get(url, headers=headers)&#xA;    res.raise_for_status()&#xA;    return res.json().get(&apos;envelopes&apos;, [])&#xA;&#xA;&#xA;def download_large_document(config, account_id, envelope_id, document_id, token):&#xA;    doc_url = f&quot;{get_base_path(config)}/v2.1/accounts/{account_id}/envelopes/{envelope_id}/documents/{document_id}&quot;&#xA;    headers = {&apos;Authorization&apos;: f&apos;Bearer {token}&apos;}&#xA;    try:&#xA;        temp_stream = download_to_tempfile(doc_url, headers)&#xA;        return temp_stream&#xA;    except Exception as e:&#xA;        logger.exception(f&quot;Failed to download document {document_id} from envelope {envelope_id}&quot;)&#xA;        raise&#xA;&#xA;&#xA;&#xA;def get_envelope_metadata(config, account_id, envelope_id, token):&#xA;    url = f&quot;{get_base_path(config)}/v2.1/accounts/{account_id}/envelopes/{envelope_id}&quot;&#xA;    headers = {&apos;Authorization&apos;: f&apos;Bearer {token}&apos;}&#xA;    res = requests.get(url, headers=headers)&#xA;    res.raise_for_status()&#xA;    return res.json()&#xA;&#xA;def get_base_path(config):&#xA;    try:&#xA;        return config[&apos;docusign&apos;][&apos;basePath&apos;]&#xA;    except KeyError:&#xA;        logger.exception(&quot;Missing &apos;basePath&apos; in DocuSign config. Please check your configuration file.&quot;)&#xA;        raise&#xA;&#xA;def get_envelope_documents(config, account_id, envelope_id, token):&#xA;    url = f&quot;{get_base_path(config)}/v2.1/accounts/{account_id}/envelopes/{envelope_id}/documents&quot;&#xA;    headers = {&apos;Authorization&apos;: f&apos;Bearer {token}&apos;}&#xA;    res = requests.get(url, headers=headers)&#xA;    res.raise_for_status()&#xA;    return res.json().get(&apos;envelopeDocuments&apos;, [])&#xA;&#xA;&#xA;def upload_to_s3(s3, bucket, key, body, content_type, is_stream=False):&#xA;    try:&#xA;        if is_stream:&#xA;            s3.upload_fileobj(body, bucket, key, ExtraArgs={&apos;ContentType&apos;: content_type})&#xA;        else:&#xA;            s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)&#xA;        logger.info(f&quot;Uploaded: {key}&quot;)&#xA;    except Exception as e:&#xA;        logger.exception(f&quot;Failed to upload {key} to S3&quot;)&#xA;        raise&#xA;&#xA;&#xA;def get_last_run_timestamp(s3, bucket, config):&#xA;    try:&#xA;        obj = s3.get_object(Bucket=bucket, Key=LAST_RUN_KEY)&#xA;        return obj[&apos;Body&apos;].read().decode().strip()&#xA;    except ClientError:&#xA;        if config.get(&apos;fromDate&apos;):&#xA;            logger.info(f&quot;Using configured fromDate: {config[&apos;fromDate&apos;]}&quot;)&#xA;            return config[&apos;fromDate&apos;]&#xA;        else:&#xA;            default_date = datetime.now(UTC) - timedelta(days=7)&#xA;            default_date = default_date.replace(microsecond=0).strftime(&quot;%Y-%m-%dT%H:%M:%S&quot;)&#xA;            logger.info(f&quot;No previous timestamp found. Using default: 7 days ago {default_date}&quot;)&#xA;            return default_date&#xA;&#xA;&#xA;def save_backup_timestamp(s3, bucket, ts):&#xA;    logger.info(f&quot;Saving last run time to {LAST_RUN_KEY} on S3&quot;)&#xA;    s3.put_object(Bucket=bucket, Key=LAST_RUN_KEY, Body=ts.encode(), ContentType=&apos;text/plain&apos;)&#xA;&#xA;&#xA;def determine_content_type(filename):&#xA;    content_type, _ = mimetypes.guess_type(filename)&#xA;    return content_type or &apos;application/octet-stream&apos;&#xA;&#xA;def download_to_tempfile(url, headers):&#xA;    response = requests.get(url, headers=headers, stream=True)&#xA;    response.raise_for_status()&#xA;&#xA;    temp_file = tempfile.NamedTemporaryFile(delete=True)&#xA;    for chunk in response.iter_content(chunk_size=8192):&#xA;        if chunk:  # filter out keep-alive chunks&#xA;            temp_file.write(chunk)&#xA;    temp_file.seek(0)&#xA;    return temp_file&#xA;&#xA;def run_backup(config, bucket):&#xA;    try:&#xA;        logger.info(&quot;----- Starting Backup -----&quot;)&#xA;        s3 = get_s3_client(config)&#xA;        token = get_access_token(config)&#xA;        account = get_user_info(config, token)&#xA;        from_date = get_last_run_timestamp(s3, bucket, config)&#xA;        logger.info(f&quot;From S3, we get last backup run time: {from_date}, will get completed envelops from docusign.&quot;)&#xA;        envelopes = list_completed_envelopes(config, account[&apos;account_id&apos;], token, from_date)&#xA;        now_str = datetime.now(UTC).strftime(&quot;%Y-%m-%dT%H:%M:%SZ&quot;)&#xA;        today = now_str.split(&quot;T&quot;)[0]&#xA;        if not envelopes:&#xA;            logger.info(&quot;There are no completed envelopes found for backup.&quot;)&#xA;        else:&#xA;            for env in envelopes:&#xA;                eid = env[&apos;envelopeId&apos;]&#xA;                logger.info(f&quot;Processing envelop: [{eid}].&quot;)&#xA;                try:&#xA;                    meta = get_envelope_metadata(config, account[&apos;account_id&apos;], eid, token)&#xA;                    logger.info(f&quot;Uploading metadata.json to s3 with key {today}/{eid}/metadata.json.&quot;)&#xA;                    upload_to_s3(s3, bucket, f&quot;{today}/{eid}/metadata.json&quot;, yaml.dump(meta).encode(), &quot;application/json&quot;)&#xA;                    logger.info(f&quot;Getting docusign documents for envelop: [{eid}].&quot;)&#xA;                    docs = get_envelope_documents(config, account[&apos;account_id&apos;], eid, token)&#xA;                    for doc in docs:&#xA;                        name = doc[&apos;name&apos;]&#xA;                        logger.info(f&quot;Processing document: [{name}].&quot;)&#xA;                        try:&#xA;                            logger.info(f&quot;Downloading document from docusign with id: [{doc[&apos;documentId&apos;]}].&quot;)&#xA;                            stream = download_large_document(config, account[&apos;account_id&apos;], eid, doc[&apos;documentId&apos;], token)&#xA;                            content_type = determine_content_type(name)&#xA;                            logger.info(f&quot;Uploading document to s3 with key {today}/{eid}/{name}.&quot;)&#xA;                            upload_to_s3(s3, bucket, f&quot;{today}/{eid}/{name}&quot;, stream, content_type, is_stream=True)&#xA;                        except Exception as e:&#xA;                            logger.error(f&quot;Unable to backup document {name} in envelope {eid} due to error: {e}. Throwing error.&quot;)&#xA;                            raise&#xA;                except Exception as e:&#xA;                    logger.error(f&quot;Failed to process envelope {eid}: {e}&quot;)&#xA;                    raise&#xA;        logger.info(f&quot;Saving backup run time: [{now_str}] to S3.&quot;)&#xA;        save_backup_timestamp(s3, bucket, now_str)&#xA;        logger.info(&quot;Backup completed.&quot;)&#xA;&#xA;    except Exception as e:&#xA;        logger.exception(&quot;Backup process failed.&quot;)&#xA;        raise&#xA;&#xA;def list_backups(config, bucket, prefix=&quot;&quot;, from_date=None):&#xA;    &quot;&quot;&quot;&#xA;    Lists all backup folders and files on or after a given date (YYYY-MM-DD).&#xA;    &quot;&quot;&quot;&#xA;    logger.info(&quot;----- Starting List Backup -----&quot;)&#xA;    &#xA;    s3 = get_s3_client(config)&#xA;&#xA;    if prefix and not prefix.endswith(&quot;/&quot;):&#xA;        prefix += &quot;/&quot;&#xA;&#xA;    paginator = s3.get_paginator(&quot;list_objects_v2&quot;)&#xA;    operation_params = {&#xA;        &quot;Bucket&quot;: bucket,&#xA;        &quot;Prefix&quot;: prefix,&#xA;        &quot;Delimiter&quot;: &quot;/&quot;&#xA;    }&#xA;&#xA;    logger.info(f&quot;Listing backup folders from: {from_date or &apos;beginning&apos;}, prefix: [{prefix}], bucket: [{bucket}]&quot;)&#xA;&#xA;    found = False&#xA;    valid_folders = []&#xA;&#xA;    for page in paginator.paginate(**operation_params):&#xA;        if &quot;CommonPrefixes&quot; in page:&#xA;            print(f&quot;Common Prefixes {str(page[&apos;CommonPrefixes&apos;])}&quot;)&#xA;            for cp in page[&quot;CommonPrefixes&quot;]:&#xA;                folder = cp[&quot;Prefix&quot;].rstrip(&quot;/&quot;)&#xA;                folder_name = folder.split(&quot;/&quot;)[-1]&#xA;                try:&#xA;                    if from_date is None or folder_name &amp;gt;= from_date:&#xA;                        valid_folders.append(folder)&#xA;                except Exception as e:&#xA;                    logger.warning(f&quot;Skipping folder {folder}: {e}&quot;)&#xA;    &#xA;    if not valid_folders:&#xA;        print(&quot;No matching backups found.&quot;)&#xA;        logger.info(&quot;No matching backups found.&quot;)&#xA;        return&#xA;&#xA;    for folder in sorted(valid_folders):&#xA;        print(f&quot;{folder}&quot;)&#xA;        logger.info(f&quot;{folder}&quot;)&#xA;&#xA;        # List contents inside the folder&#xA;        inner_params = {&#xA;            &quot;Bucket&quot;: bucket,&#xA;            &quot;Prefix&quot;: folder + &quot;/&quot;&#xA;        }&#xA;        for subpage in paginator.paginate(**inner_params):&#xA;            if &quot;Contents&quot; in subpage:&#xA;                for obj in subpage[&quot;Contents&quot;]:&#xA;                    if not obj[&quot;Key&quot;].endswith(&quot;/&quot;):&#xA;                        print(f&quot;   &#x2514;&#x2500;&#x2500; {obj[&apos;Key&apos;].split(folder + &apos;/&apos;)[1]}&quot;)&#xA;                        logger.info(f&quot;   &#x2514;&#x2500;&#x2500; {obj[&apos;Key&apos;]}&quot;)&#xA;&#xA;    logger.info(&quot;----- END List Backup -----&quot;)&#xA;&#xA;def restore_backup(config, bucket, prefix):&#xA;    logger.info(&quot;----- Starting Restore From Backup -----&quot;)&#xA;    logger.info(&quot;Files will be restored to &apos;restored/&apos; directory.&quot;)&#xA;    try:&#xA;        s3 = get_s3_client(config)&#xA;        paginator = s3.get_paginator(&quot;list_objects_v2&quot;)&#xA;        logger.info(f&quot;Restoring backup from: {prefix or &apos;beginning&apos;}&quot;)&#xA;        download_root = &quot;restored&quot;&#xA;&#xA;        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):&#xA;            for obj in page.get(&quot;Contents&quot;, []):&#xA;                key = obj[&quot;Key&quot;]&#xA;&#xA;                # Build relative path under &apos;restored/&apos; based on full key&#xA;                local_path = os.path.join(download_root, *key.split(&apos;/&apos;))&#xA;&#xA;                # Ensure folders exist, not including the filename&#xA;                os.makedirs(os.path.dirname(local_path), exist_ok=True)&#xA;&#xA;                # Download file&#xA;                with open(local_path, &quot;wb&quot;) as f:&#xA;                    body = s3.get_object(Bucket=bucket, Key=key)[&quot;Body&quot;].read()&#xA;                    f.write(body)&#xA;&#xA;                logger.info(f&quot;Restored: {key} --&amp;gt; {local_path}&quot;)&#xA;    except Exception as e:&#xA;        logger.exception(f&quot;Exception while restore from  {prefix}: {e}&quot;)&#xA;    logger.info(&quot;----- End Restore From Backup -----&quot;)&#xA;&#xA;&#xA;&#xA;exitCode = 0&#xA;try:&#xA;    logger.info(&quot;----- Starting DocuSign Backup -----&quot;)&#xA;    config = load_config_from_json(r&quot;&quot;&quot;xpath:{/workflow/configuration/configJson}&quot;&quot;&quot;)&#xA;    logger.info(&quot;Config loaded from JSON string.&quot;)&#xA;    #print(f&quot;Using config: {config}&quot;)&#xA;    if not config:&#xA;        print(f&quot;Failed to load config from JSON string. Please save configJson in the workflow configuration.&quot;)&#xA;        logger.error(f&quot;Failed to load config from JSON string. Please save configJson in the workflow configuration.&quot;)&#xA;        exitCode = 1&#xA;    else:&#xA;        bucket = config[&quot;aws&quot;][&quot;bucket&quot;]&#xA;        run_backup(config, bucket)&#xA;except Exception as e:&#xA;    logger.exception(f&quot;An error occurred during the DocuSign backup process: {e}&quot;)&#xA;    exitCode = 1&#xA;finally:&#xA;    logger.info(&quot;----- END DocuSign Backup -----&quot;)&#xA;    sys.exit(exitCode)&#xA;&lt;/script&gt;&#xA;  &lt;arguments class=&quot;java.lang.String&quot; _list_=&quot;false&quot;/&gt;&#xA;  &lt;waitForProcessCompletion class=&quot;java.lang.Boolean&quot; _list_=&quot;false&quot;&gt;true&lt;/waitForProcessCompletion&gt;&#xA;  &lt;client&gt;&#xA;    &lt;clientName/&gt;&#xA;    &lt;clientId/&gt;&#xA;    &lt;hostName/&gt;&#xA;    &lt;clientGUID/&gt;&#xA;    &lt;displayName/&gt;&#xA;    &lt;commCellName/&gt;&#xA;    &lt;csGUID/&gt;&#xA;    &lt;type/&gt;&#xA;    &lt;flags/&gt;&#xA;    &lt;GUID/&gt;&#xA;    &lt;newName/&gt;&#xA;  &lt;/client&gt;&#xA;&lt;/inputs&gt;" /><onExit language="3" script="var exitcode = xpath:{/workflow/ExecuteScript_3/exitCode};&#xA;if(exitcode &amp;&amp; exitcode === 0){&#xA;&#x9;workflow.setVariable(&quot;exitCode&quot;, 0);&#xA;}else{&#xA;  &#x9;workflow.setVariable(&quot;exitCode&quot;, 3);&#xA;&#x9;workflow.setVariable(&quot;message&quot;, &quot;There are some error while running docusing backup, please check docusign_backup.log for more details. Log file will be available under Commvault install dir/Log Files/ folder on the workflow engine machine.&quot;);&#xA;&#xA;}&#xA;&#xA;" /><activitySchema><outputs><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" documentation="the exitCode recieved from executing the command" name="exitCode" /><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" documentation="the return code recieved from completion of the command" name="errorCode" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" documentation="the output from the command or error message if it failed" name="commandOutput" /></outputs><inputs type=""><children className="commvault.msgs.CvEntities.ClientEntity" type="{commvault.msgs.CvEntities}ClientEntity" inputType="commvault.msgs.CvEntities.ClientEntity" documentation="the remote host to execute the command on" name="client"><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="clientName" /><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" name="clientId" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="clientName" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="hostName" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="clientGUID" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="displayName" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="commCellName" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="csGUID" /><children className="commvault.msgs.CvEntities.CommCellType" type="{commvault.msgs.CvEntities}CommCellType" inputType="commvault.msgs.CvEntities.CommCellType" name="type"><options val="GALAXY" /><options val="NBU" /><options val="NETAPP" /><options val="TSM" /><options val="VEEAM" /></children><children className="commvault.msgs.CvEntities.EntityFlags" type="{commvault.msgs.CvEntities}EntityFlags" inputType="commvault.msgs.CvEntities.EntityFlags" name="flags" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="GUID" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" name="newName" /></children><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" required="1" inputType="java.lang.String" documentation="the host username to execute the command as" name="impersonateUserName" /><children className="workflow.types.EncryptedString" type="{workflow.types}EncryptedString" required="1" inputType="workflow.types.EncryptedString" documentation="the credentials to execute the command as" name="impersonateUserPassword" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" required="1" inputType="java.lang.String" documentation="the path to the executable" name="startUpPath" /><children className="commvault.msgs.App.ScriptType" type="{commvault.msgs.App}ScriptType" required="1" inputType="commvault.msgs.App.ScriptType" name="scriptType"><options val="Java" /><options val="Python" /><options val="PowerShell" /><options val="WindowsBatch" /><options val="UnixShell" /><options val="Perl" /></children><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" required="1" inputType="java.lang.String" name="script" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" documentation="arguments to pass into command" name="arguments" /><children defaultValue="true" className="java.lang.Boolean" type="{http://www.w3.org/2001/XMLSchema}boolean" inputType="java.lang.Boolean" documentation="tells the activity to wait for process to complete and retrieve the output from the command" name="waitForProcessCompletion" /></inputs></activitySchema></Activity><Activity displayName="Break" description="interrupts a process block execution" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="35" created="1756103402678" breakpoint="0" uniqueName="Break_1" name="Break" width="80" x="291" y="38"><activitySchema><outputs /><inputs /></activitySchema></Activity></superProcess><activitySchema><inputs /></activitySchema></Activity><Activity maxRestarts="0" displayName="Process Backup" description="executes a defined process block within the workflow" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="23" created="1756103634578" breakpoint="0" uniqueName="ExecuteProcessBlock_1" name="ExecuteProcessBlock" width="106" x="410" y="108"><inputs val="&lt;inputs&gt;&#xA;  &lt;inputs/&gt;&#xA;  &lt;processBlock class=&quot;commvault.cte.workflow.dom.WorkflowElement&quot; _list_=&quot;false&quot;&gt;ProcessBlock_1&lt;/processBlock&gt;&#xA;  &lt;outputs class=&quot;commvault.cte.workflow.dom.WorkflowElement&quot; _list_=&quot;false&quot;/&gt;&#xA;&lt;/inputs&gt;" /><activitySchema><outputs><children name="Start_2" /><children name="ExecuteScript_2"><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" documentation="the exitCode recieved from executing the command" name="exitCode" /><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" documentation="the return code recieved from completion of the command" name="errorCode" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" documentation="the output from the command or error message if it failed" name="commandOutput" /></children><children name="ExecuteScript_3"><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" documentation="the exitCode recieved from executing the command" name="exitCode" /><children className="java.lang.Integer" type="{http://www.w3.org/2001/XMLSchema}integer" inputType="java.lang.Integer" documentation="the return code recieved from completion of the command" name="errorCode" /><children className="java.lang.String" type="{http://www.w3.org/2001/XMLSchema}string" inputType="java.lang.String" documentation="the output from the command or error message if it failed" name="commandOutput" /></children><children name="Break_1" /></outputs><inputs /></activitySchema><transition activity="ReleaseLock_1" points="374,117" value="ANY" /></Activity><formProperties /><minCommCellVersion servicePack="0" releaseID="16" /><globalConfigInfo /></Workflow_WorkflowDefinition>"""
DOCUSIGN_VAULT_NAME = "delete-chandan-test3"
DOCUSIGN_WORKFLOW_NAME = "Backup Docusign"


def _check_workflow_exists(
    workflow_name: Annotated[str, Field(description="Name of the workflow to check for existence.")],
):
    workflow_list = commvault_api_client.get("Workflow")
    if isinstance(workflow_list, dict) and "container" in workflow_list:
        for workflow in workflow_list["container"]:
            if isinstance(workflow, dict) and "entity" in workflow:
                entity = workflow["entity"]
                if entity.get("workflowName") == workflow_name:
                    return {"workflowId": entity.get("workflowId")}
    return None

def _trigger_workflow(workflow_name: Annotated[str, Field(description="Name of the workflow to trigger.")]):
    response = commvault_api_client.post(f"wapi/{workflow_name}")
    if isinstance(response, dict) and "jobId" in response:
        return {"message": f"Backup triggered successfully. Job ID: {response['jobId']}"}
    else:
        return response

def _import_and_deploy_workflow():
    new_workflow = commvault_api_client.put("Workflow", data=DOCUSIGN_BACKUP_WORKFLOW_XML)

    if not isinstance(new_workflow, dict) or "workflow" not in new_workflow:
        return {"error": "Workflow creation failed"}
    
    workflow_info = new_workflow["workflow"]
    required_fields = ["GUID", "workflowName", "workflowId"]
    
    if all(workflow_info.get(field) for field in required_fields):
        deployment_result = commvault_api_client.post(f"Workflow/{workflow_info['workflowId']}/Action/Deploy?clientId=2")
        if (isinstance(deployment_result, dict) and deployment_result.get("errorMessage") == "Success"):
            if _update_workflow_configuration(workflow_info["workflowId"]):
                return workflow_info["workflowName"]
    
    return {"error": "Workflow creation failed"}

def _update_workflow_configuration(workflow_id: Annotated[int, Field(description="ID of the workflow to update.")]):
    config_path = "config/docusign_config.json"
    private_key_path = "config/docusign_key.pem"
    if not os.path.isfile(config_path) or not os.path.isfile(private_key_path):
        raise Exception("Docusign Configuration file not found. Please add a docusign_config.json and docusign_key.pem file in config folder using the format specified in the documentation.")

    with open(config_path, "r", encoding="utf-8") as config_file:
        config_json = config_file.read()

    with open(private_key_path, "r", encoding="utf-8") as private_key_file:
        private_key = private_key_file.read()

    xml_payload = f"""
    <configuration>
        <configJson><![CDATA[{config_json}]]></configJson>
        <privateKey><![CDATA[{private_key}]]></privateKey>
    </configuration>
    """
    xml_payload = xml_payload.strip()

    response = commvault_api_client.post(f"cr/apps/configform/{workflow_id}", data=xml_payload)
    if isinstance(response, dict) and response.get("errorCode") == 0:
        return True
    else:
        return False
    
def _ensure_docusign_workflow_exists():
    workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)
    
    if not workflow_entity:
        workflow_name = _import_and_deploy_workflow()
        if isinstance(workflow_name, dict) and "error" in workflow_name:
            raise Exception(workflow_name["error"])
    
    return True

def _create_docusign_backup_vault():
    payload = {
        "bucket": {
            "clientName": DOCUSIGN_VAULT_NAME
        },
        "plan": {
            "planId": 3,
            "planName": "aravind-sp"
        },
        "owners": [
            {
            "userId": 4,
            "userName": "admin"
            }
        ]
    }

    response = commvault_api_client.post(f"v4/cvs3Store", data=payload)

    if isinstance(response, dict) and response.get("response", {}).get("errorCode", 1) == 0:
        return True
    else:
        raise Exception(f"Failed to create backup vault: {response}")
    
def _get_and_set_vault_keys():
    response = commvault_api_client.put(f"v4/user/4/s3accesskey")
    if (
        isinstance(response, dict)
        and response.get("response", {}).get("errorCode") == 200
        and "accessKeyID" in response
        and "secretAccessKey" in response
    ):
        config_path = "config/docusign_config.json"
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            raise Exception("DocuSign configuration file not found. Please ensure config/docusign_config.json exists.")
        except json.JSONDecodeError:
            raise Exception("Invalid JSON in DocuSign configuration file.")
        
        if "aws" not in config:
            config["aws"] = {}
        
        config["aws"]["bucket"] = DOCUSIGN_VAULT_NAME
        config["aws"]["accessKeyId"] = response["accessKeyID"]
        config["aws"]["secretAccessKey"] = response["secretAccessKey"]
        
        try:
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=3)
            logger.info(f"Successfully updated AWS credentials in {config_path}")
            return {
                "status": "success",
                "message": f"AWS credentials updated in {config_path}",
                "accessKeyID": response["accessKeyID"],
                "secretAccessKey": response["secretAccessKey"]
            }
        except Exception as e:
            raise Exception(f"Failed to write updated config to {config_path}: {str(e)}")
    else:
        raise Exception(f"Failed to get vault keys: {response}")

def setup_docusign_backup_vault():
    """
    Use this tool to setup a vault to backup Docusign. Use this tool only if the user specifically requests to set it up. Or else assume that the user has already set it up.
    You must confirm from the user before executing this.
    """
    # _create_docusign_backup_vault()
    # _get_and_set_vault_keys()
    _ensure_docusign_workflow_exists()

    return {"message": f"Docusign backup is set up successfully. You can now trigger or schedule backups."}

def trigger_docusign_backup() -> dict:
    """
    Triggers a workflow to backup Docusign documents. Assume that the user has already set up the backup vault. If it does not exist, an error will be raised.
    """
    try:
        workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)

        if not workflow_entity:
            raise Exception("Docusign backup is not configured. Please setup a vault to run backups.")

        return _trigger_workflow(DOCUSIGN_WORKFLOW_NAME)
    except Exception as e:
        logger.error(f"Error triggering workflow {DOCUSIGN_WORKFLOW_NAME}: {e}")
        return ToolError({"error": str(e)})

def schedule_docusign_backup(
    schedule_type: Annotated[str, Field(description="Type of schedule to create. Options are 'daily' or 'weekly'. Default is 'daily'.")] = "daily",
    time: Annotated[str, Field(description="Time to run the backup job. 24 Hour Format: HH:MM")] = "18:00",
    day_of_week: Annotated[str, Field(description="Day of the week to run the backup job if the schedule type is 'weekly'. Default is 'Sunday'. Title case is used for days.")] = "Sunday"
):
    """
    Schedules docusign backup workflow to run daily. Ask the user if they want to schedule it before running.
    """
    schedule_type = 'daily'

    if schedule_type not in ("daily", "weekly"):
        return {"error": "Only 'daily' and 'weekly' schedule types are supported."}

    hour, minute = map(int, time.split(":"))
    trigger_time = hour*3600 + minute*60

    workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)
    if workflow_entity and "workflowId" in workflow_entity:
        workflow_id = workflow_entity["workflowId"]
    else:
        return {"error": "Workflow not found."}

    if schedule_type == "daily":
        freq_type = 4  # Daily
        freq_interval = 1
        days_to_run = None
    else:
        freq_type = 8  # Weekly
        freq_interval = 127  # All days
        days_to_run = {d: d == day_of_week for d in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]}

    schedule_name = f"{DOCUSIGN_WORKFLOW_NAME}-{schedule_type}-schedule"

    pattern = {
        "name": schedule_name,
        "freq_type": freq_type,
        "freq_interval": freq_interval,
        "freq_recurrence_factor": 1,
        "active_start_time": trigger_time,
        "timeZone": {
            "TimeZoneID": 1000
        },
        "description": f"{schedule_type} schedule for {DOCUSIGN_WORKFLOW_NAME}"
    }
    if days_to_run:
        pattern["daysToRun"] = days_to_run

    payload = {
        "taskInfo": {
            "subTasks": [
                {
                    "subTask": {
                        "subTaskName": schedule_name,
                        "subTaskType": 1,
                        "operationType": 2001,
                        "flags": 0
                    },
                    "pattern": pattern,
                    "options": {
                        "workflowJobOptions": "<inputs></inputs>"
                    }
                }
            ],
            "task": {
                "taskType": 2,
                "initiatedFrom": 1,
                "policyType": 0
            },
            "associations": [
                {
                    "workflowId": workflow_id
                }
            ]
        }
    }
    response = commvault_api_client.post("CreateTask", data=payload)
    if isinstance(response, dict) and "taskId" in response:
        return {"message": f"Schedule created successfully. Task ID: {response['taskId']}"}
    return response

def get_docusign_backup_jobs(
    jobLookupWindow: Annotated[int, Field(description="The time window in seconds to look up for jobs jobs. For example, 86400 for the last 24 hours.")]=86400,
    limit: Annotated[int, Field(description="The maximum number of jobs to return. Default is 50.")] = 50,
    offset: Annotated[int, Field(description="The offset for pagination.")] = 0
):
    """
    Retrieves the list of Docusign backup jobs.
    """
    workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)
    if not workflow_entity:
        return {"error": "Workflow not found."}

    workflow_id = workflow_entity["workflowId"]
    endpoint = f"cr/reportsplusengine/datasets/e8ee6af4-58d8-4444-abae-3c096e5628a4/data?limit={limit}&offset={offset}&orderby=%5BjobEndTime%5DDESC&parameter.hideAdminJobs=0&parameter.jobCategory=3&parameter.showOnlyLaptopJobs=0&parameter.completedJobLookupTime={jobLookupWindow}&parameter.workFlows={workflow_id}&parameter.jobTypes=90"
    response = commvault_api_client.get(endpoint)
    formatted_response = format_report_dataset_response(response)
    
    # Filter the response to keep only important fields for LLM
    if isinstance(formatted_response, dict) and "records" in formatted_response:
        important_fields = [
            "jobId",
            "status",
            "percentComplete",
            "jobStartTime",
            "jobEndTime", 
            "jobElapsedTime",
            "pendingReason"
        ]
        
        filtered_records = []
        for record in formatted_response["records"]:
            filtered_record = {field: record.get(field) for field in important_fields if record.get(field) is not None}
            filtered_records.append(filtered_record)
        
        formatted_response["records"] = filtered_records
    
    return formatted_response

def list_backedup_docusign_envelopes(
    date: Annotated[str, Field(description="Date in YYYY-MM-DD format to list backups on specific date. Default is empty string which lists all backups.")] = ""
):
    """
    Lists all backed up docusign envelopes and files. This can be used to fetch the paths of all backed up envelopes and their files.
    """
    def _get_s3_client(config):
        try:
            return boto3.client(
                's3',
                endpoint_url=config['aws']['endpoint'],
                region_name=config['aws']['region'],
                aws_access_key_id=config['aws']['accessKeyId'],
                aws_secret_access_key=config['aws']['secretAccessKey'],
                config=boto3.session.Config(s3={'addressing_style': 'path'})
            )
        except KeyError as e:
            logger.exception(f"Missing S3 Vault config key: {e}. Please check your configuration file.")
            raise

    def _process_envelope(envelope_folder):
        """Process a single envelope and return envelope data"""
        envelope_id = envelope_folder.split("/")[-1]
        logger.info(f"Processing envelope: {envelope_id}")
        
        document_files = []
        envelope_params = {
            "Bucket": DOCUSIGN_VAULT_NAME,
            "Prefix": envelope_folder + "/"
        }
        
        for envelope_page in paginator.paginate(**envelope_params):
            if "Contents" in envelope_page:
                for obj in envelope_page["Contents"]:
                    if not obj["Key"].endswith("/"):
                        file_name = obj["Key"].split(envelope_folder + "/")[1]
                        if file_name not in ("metadata.json", "Summary"):
                            document_files.append(file_name)
                        logger.info(f"   |___ {obj['Key']}")
        
        envelope_entry = {"id": envelope_id}
        if document_files:
            if len(document_files) <= 10:
                envelope_entry["docs"] = document_files
            else:
                envelope_entry["docs"] = f"{len(document_files)} documents"
        
        return envelope_entry

    def _process_date_folder(folder):
        """Process a date folder and return date entry with envelopes"""
        folder_name = folder.split("/")[-1]
        logger.info(f"Processing date folder: {folder}")
        
        date_entry = {
            "date": folder_name,
            "envelopes": []
        }
        
        date_params = {
            "Bucket": DOCUSIGN_VAULT_NAME,
            "Prefix": folder + "/",
            "Delimiter": "/"
        }
        
        for date_page in paginator.paginate(**date_params):
            if "CommonPrefixes" in date_page:
                for envelope_cp in date_page["CommonPrefixes"]:
                    envelope_folder = envelope_cp["Prefix"].rstrip("/")
                    envelope_entry = _process_envelope(envelope_folder)
                    date_entry["envelopes"].append(envelope_entry)
        
        return date_entry if date_entry["envelopes"] else None
    
    logger.info("----- Starting List Backup -----")

    config_path = "config/docusign_config.json"
    if not os.path.isfile(config_path):
        raise Exception("Docusign Configuration file not found")

    with open(config_path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    logger.info(config)
    s3 = _get_s3_client(config)
    paginator = s3.get_paginator("list_objects_v2")

    backup_data = {"backups": []}

    if date:
        # When date is specified, process that specific date
        if not date.endswith("/"):
            date += "/"
        
        logger.info(f"Listing backup for specific date: [{date}], bucket: [{DOCUSIGN_VAULT_NAME}]")
        
        date_params = {
            "Bucket": DOCUSIGN_VAULT_NAME,
            "Prefix": date,
            "Delimiter": "/"
        }
        
        date_entry = {
            "date": date.rstrip("/"),
            "envelopes": []
        }
        
        for date_page in paginator.paginate(**date_params):
            if "CommonPrefixes" in date_page:
                for envelope_cp in date_page["CommonPrefixes"]:
                    envelope_folder = envelope_cp["Prefix"].rstrip("/")
                    envelope_entry = _process_envelope(envelope_folder)
                    date_entry["envelopes"].append(envelope_entry)
        
        if date_entry["envelopes"]:
            backup_data["backups"].append(date_entry)
    else:
        # When no date specified, list all dates
        operation_params = {
            "Bucket": DOCUSIGN_VAULT_NAME,
            "Prefix": "",
            "Delimiter": "/"
        }

        logger.info(f"Listing all backup folders, bucket: [{DOCUSIGN_VAULT_NAME}]")

        for page in paginator.paginate(**operation_params):
            if "CommonPrefixes" in page:
                for cp in page["CommonPrefixes"]:
                    folder = cp["Prefix"].rstrip("/")
                    folder_name = folder.split("/")[-1]
                    
                    # Only consider date folders (skip docusign-backup folder)
                    if folder_name != "docusign-backup":
                        date_entry = _process_date_folder(folder)
                        if date_entry:
                            backup_data["backups"].append(date_entry)

    total_dates = len(backup_data["backups"])
    total_envelopes = sum(len(date_data["envelopes"]) for date_data in backup_data["backups"])
    backup_data["summary"] = f"{total_dates} dates, {total_envelopes} envelopes"

    if total_dates == 0:
        logger.info("No matching backups found.")
        backup_data["message"] = "No backups found"
    else:
        logger.info(f"Found {total_dates} backup dates with {total_envelopes} envelopes")

    logger.info("----- END List Backup -----")
    logger.info(json.dumps(backup_data, indent=2))
    return backup_data

DOCUSIGN_TOOLS = [
    setup_docusign_backup_vault,
    trigger_docusign_backup,
    schedule_docusign_backup,
    get_docusign_backup_jobs,
    list_backedup_docusign_envelopes
]
