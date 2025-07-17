import os
import re
from pathlib import Path
from typing import List, Optional, Tuple, Dict
from datetime import datetime
import xml.etree.ElementTree as ET

import git
from git import Repo, GitCommandError
from loguru import logger
import streamlit as st


class GitManager:
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.repo: Optional[Repo] = None
        self._initialize_repo()
    
    def _initialize_repo(self):
        try:
            if self.repo_path.exists() and (self.repo_path / '.git').exists():
                self.repo = Repo(self.repo_path)
                logger.info(f"Initialized Git repository at {self.repo_path}")
            else:
                logger.error(f"Git repository not found at {self.repo_path}")
                st.error(f"Git repository not found at {self.repo_path}")
        except Exception as e:
            logger.error(f"Failed to initialize Git repository: {e}")
            st.error(f"Failed to initialize Git repository: {e}")
    
    def validate_git_setup(self) -> Tuple[bool, str]:
        if not self.repo:
            return False, "Git repository not initialized"
        
        try:
            # Check if repo is clean or has only untracked files
            if self.repo.is_dirty():
                return False, "Repository has uncommitted changes"
            
            # Check if we can access remotes
            try:
                remotes = list(self.repo.remotes)
                if not remotes:
                    logger.warning("No remotes configured")
            except Exception as e:
                logger.warning(f"Could not access remotes: {e}")
            
            # Check current branch
            try:
                current_branch = self.repo.active_branch.name
                logger.info(f"Current branch: {current_branch}")
            except Exception as e:
                logger.warning(f"Could not determine current branch: {e}")
            
            return True, "Git setup is valid"
            
        except Exception as e:
            logger.error(f"Git validation failed: {e}")
            return False, f"Git validation failed: {e}"
    
    def create_feature_branch(self, branch_name: str, base_branch: str = 'dev') -> Tuple[bool, str]:
        if not self.repo:
            return False, "Git repository not initialized"
        
        try:
            # Clean branch name
            clean_branch_name = self._clean_branch_name(branch_name)
            
            # Check if branch already exists
            if clean_branch_name in [b.name for b in self.repo.branches]:
                return False, f"Branch '{clean_branch_name}' already exists"
            
            # Try to checkout base branch first
            try:
                base_ref = None
                
                # Try local branch first
                if base_branch in [b.name for b in self.repo.branches]:
                    base_ref = self.repo.branches[base_branch]
                # Try remote branch
                elif f'origin/{base_branch}' in [b.name for b in self.repo.remotes.origin.refs]:
                    base_ref = self.repo.remotes.origin.refs[base_branch]
                # Try main as fallback
                elif 'main' in [b.name for b in self.repo.branches]:
                    base_ref = self.repo.branches['main']
                    logger.warning(f"Base branch '{base_branch}' not found, using 'main'")
                # Try master as fallback
                elif 'master' in [b.name for b in self.repo.branches]:
                    base_ref = self.repo.branches['master']
                    logger.warning(f"Base branch '{base_branch}' not found, using 'master'")
                
                if not base_ref:
                    return False, f"Could not find base branch '{base_branch}' or fallback branches"
                
                # Create and checkout new branch
                new_branch = self.repo.create_head(clean_branch_name, base_ref)
                new_branch.checkout()
                
                logger.info(f"Created and checked out branch '{clean_branch_name}' from '{base_ref.name}'")
                return True, f"Successfully created branch '{clean_branch_name}'"
                
            except Exception as e:
                logger.error(f"Failed to create branch: {e}")
                return False, f"Failed to create branch: {e}"
                
        except Exception as e:
            logger.error(f"Branch creation failed: {e}")
            return False, f"Branch creation failed: {e}"
    
    def _clean_branch_name(self, branch_name: str) -> str:
        # Remove invalid characters and format for Git
        clean_name = re.sub(r'[^a-zA-Z0-9_\-/]', '_', branch_name)
        clean_name = re.sub(r'_+', '_', clean_name)  # Collapse multiple underscores
        clean_name = clean_name.strip('_')  # Remove leading/trailing underscores
        
        # Add timestamp to make it unique
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        return f"migration/{clean_name}_{timestamp}"
    
    def create_directory_structure(self, database_name: str) -> Tuple[bool, str]:
        try:
            base_path = self.repo_path / database_name
            
            # Create directory structure
            directories = [
                base_path,
                base_path / "temp_schema" / "Tables",
                base_path / "temp_schema" / "Views",
                base_path / "bronze_schema" / "Tables",
                base_path / "bronze_schema" / "Views"
            ]
            
            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {directory}")
            
            return True, f"Created directory structure for {database_name}"
            
        except Exception as e:
            logger.error(f"Failed to create directory structure: {e}")
            return False, f"Failed to create directory structure: {e}"
    
    def write_files(self, files_to_create: List[Tuple[str, str]], database_name: str) -> Tuple[bool, str, List[str]]:
        if not self.repo:
            return False, "Git repository not initialized", []
        
        created_files = []
        
        try:
            for file_path, content in files_to_create:
                full_path = self.repo_path / database_name / file_path
                
                # Create parent directory if it doesn't exist
                full_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Write file
                with open(full_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                created_files.append(str(full_path.relative_to(self.repo_path)))
                logger.info(f"Created file: {full_path}")
            
            return True, f"Successfully created {len(created_files)} files", created_files
            
        except Exception as e:
            logger.error(f"Failed to write files: {e}")
            return False, f"Failed to write files: {e}", created_files
    
    def update_sqlproj(self, database_name: str, sqlproj_entries: List[str]) -> Tuple[bool, str]:
        if not self.repo:
            return False, "Git repository not initialized"
        
        try:
            sqlproj_path = self.repo_path / database_name / f"{database_name}.sqlproj"
            
            if not sqlproj_path.exists():
                # Create a new .sqlproj file
                success, message = self._create_sqlproj_file(sqlproj_path, database_name, sqlproj_entries)
                return success, message
            else:
                # Update existing .sqlproj file
                success, message = self._update_existing_sqlproj(sqlproj_path, sqlproj_entries)
                return success, message
                
        except Exception as e:
            logger.error(f"Failed to update .sqlproj file: {e}")
            return False, f"Failed to update .sqlproj file: {e}"
    
    def _create_sqlproj_file(self, sqlproj_path: Path, database_name: str, entries: List[str]) -> Tuple[bool, str]:
        try:
            sqlproj_content = f"""<?xml version="1.0" encoding="utf-8"?>
<Project DefaultTargets="Build" xmlns="http://schemas.microsoft.com/developer/msbuild/2003" ToolsVersion="4.0">
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <Name>{database_name}</Name>
    <SchemaVersion>2.0</SchemaVersion>
    <ProjectVersion>4.1</ProjectVersion>
    <ProjectGuid>{{12345678-1234-1234-1234-123456789012}}</ProjectGuid>
    <DSP>Microsoft.Data.Tools.Schema.Sql.Sql130DatabaseSchemaProvider</DSP>
    <OutputType>Database</OutputType>
    <RootPath>
    </RootPath>
    <RootNamespace>{database_name}</RootNamespace>
    <AssemblyName>{database_name}</AssemblyName>
    <ModelCollation>1033, CI</ModelCollation>
    <DefaultFileStructure>BySchemaAndSchemaType</DefaultFileStructure>
    <DeployToDatabase>True</DeployToDatabase>
    <TargetFrameworkVersion>v4.5</TargetFrameworkVersion>
    <TargetLanguage>CS</TargetLanguage>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <SqlServerVerification>False</SqlServerVerification>
    <IncludeCompositeObjects>True</IncludeCompositeObjects>
    <TargetDatabaseSet>True</TargetDatabaseSet>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <OutputPath>bin\\Release\\</OutputPath>
    <BuildScriptName>$(MSBuildProjectName).sql</BuildScriptName>
    <TreatWarningsAsErrors>False</TreatWarningsAsErrors>
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <DefineDebug>false</DefineDebug>
    <DefineTrace>true</DefineTrace>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <OutputPath>bin\\Debug\\</OutputPath>
    <BuildScriptName>$(MSBuildProjectName).sql</BuildScriptName>
    <TreatWarningsAsErrors>false</TreatWarningsAsErrors>
    <DebugSymbols>true</DebugSymbols>
    <DebugType>full</DebugType>
    <Optimize>false</Optimize>
    <DefineDebug>true</DefineDebug>
    <DefineTrace>true</DefineTrace>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup>
    <VisualStudioVersion Condition="'$(VisualStudioVersion)' == ''">11.0</VisualStudioVersion>
    <!-- Default to the v11.0 targets path if the targets file for the current VS version is not found -->
    <SSDTExists Condition="Exists('$(MSBuildExtensionsPath)\\Microsoft\\VisualStudio\\v$(VisualStudioVersion)\\SSDT\\Microsoft.Data.Tools.Schema.SqlTasks.targets')">True</SSDTExists>
    <VisualStudioVersion Condition="'$(SSDTExists)' == ''">11.0</VisualStudioVersion>
  </PropertyGroup>
  <Import Condition="'$(SQLDBExtensionsRefPath)' != ''" Project="$(SQLDBExtensionsRefPath)\\Microsoft.Data.Tools.Schema.SqlTasks.targets" />
  <Import Condition="'$(SQLDBExtensionsRefPath)' == ''" Project="$(MSBuildExtensionsPath)\\Microsoft\\VisualStudio\\v$(VisualStudioVersion)\\SSDT\\Microsoft.Data.Tools.Schema.SqlTasks.targets" />
  <ItemGroup>
    <Folder Include="Properties" />
    <Folder Include="temp_schema" />
    <Folder Include="temp_schema\\Tables" />
    <Folder Include="temp_schema\\Views" />
    <Folder Include="bronze_schema" />
    <Folder Include="bronze_schema\\Tables" />
    <Folder Include="bronze_schema\\Views" />
  </ItemGroup>
  <ItemGroup>
{"".join(f"    {entry}" + chr(10) for entry in entries)}
  </ItemGroup>
</Project>"""
            
            with open(sqlproj_path, 'w', encoding='utf-8') as f:
                f.write(sqlproj_content)
            
            logger.info(f"Created new .sqlproj file: {sqlproj_path}")
            return True, f"Created new .sqlproj file: {sqlproj_path}"
            
        except Exception as e:
            logger.error(f"Failed to create .sqlproj file: {e}")
            return False, f"Failed to create .sqlproj file: {e}"
    
    def _update_existing_sqlproj(self, sqlproj_path: Path, entries: List[str]) -> Tuple[bool, str]:
        try:
            # Read existing file
            with open(sqlproj_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse XML
            tree = ET.parse(sqlproj_path)
            root = tree.getroot()
            
            # Find or create ItemGroup for Build includes
            build_item_group = None
            for item_group in root.findall('.//{http://schemas.microsoft.com/developer/msbuild/2003}ItemGroup'):
                if item_group.find('.//{http://schemas.microsoft.com/developer/msbuild/2003}Build') is not None:
                    build_item_group = item_group
                    break
            
            if build_item_group is None:
                # Create new ItemGroup for Build includes
                build_item_group = ET.SubElement(root, 'ItemGroup')
            
            # Add new entries
            namespace = {'': 'http://schemas.microsoft.com/developer/msbuild/2003'}
            for entry in entries:
                # Parse the entry to get the Include attribute
                entry_clean = entry.strip()
                if entry_clean.startswith('<Build Include="') and entry_clean.endswith('" />'):
                    include_path = entry_clean[15:-4]  # Extract path from '<Build Include="path" />'
                    
                    # Check if entry already exists
                    existing = build_item_group.find(f'.//{{{namespace[""]}}}Build[@Include="{include_path}"]')
                    if existing is None:
                        build_element = ET.SubElement(build_item_group, 'Build')
                        build_element.set('Include', include_path)
            
            # Write back to file
            tree.write(sqlproj_path, encoding='utf-8', xml_declaration=True)
            
            logger.info(f"Updated .sqlproj file: {sqlproj_path}")
            return True, f"Updated .sqlproj file with {len(entries)} entries"
            
        except Exception as e:
            logger.error(f"Failed to update .sqlproj file: {e}")
            return False, f"Failed to update .sqlproj file: {e}"
    
    def commit_changes(self, message: str, files: List[str]) -> Tuple[bool, str]:
        if not self.repo:
            return False, "Git repository not initialized"
        
        try:
            # Add files to staging
            for file_path in files:
                self.repo.index.add([file_path])
            
            # Also add .sqlproj file if it exists
            for file_path in files:
                if file_path.endswith('.sql'):
                    # Look for corresponding .sqlproj file
                    parts = file_path.split('/')
                    if len(parts) >= 3:  # database/schema/Tables/file.sql
                        database_name = parts[0]
                        sqlproj_path = f"{database_name}/{database_name}.sqlproj"
                        if (self.repo_path / sqlproj_path).exists():
                            self.repo.index.add([sqlproj_path])
                        break
            
            # Commit changes
            commit_message = f"""{message}

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
            
            commit = self.repo.index.commit(commit_message)
            
            logger.info(f"Committed changes: {commit.hexsha}")
            return True, f"Successfully committed changes: {commit.hexsha[:8]}"
            
        except Exception as e:
            logger.error(f"Failed to commit changes: {e}")
            return False, f"Failed to commit changes: {e}"
    
    def get_current_branch(self) -> Optional[str]:
        if not self.repo:
            return None
        
        try:
            return self.repo.active_branch.name
        except Exception as e:
            logger.error(f"Failed to get current branch: {e}")
            return None
    
    def get_git_status(self) -> Dict[str, List[str]]:
        if not self.repo:
            return {'error': ['Git repository not initialized']}
        
        try:
            status = {
                'staged': [],
                'unstaged': [],
                'untracked': []
            }
            
            # Get staged files
            staged_files = self.repo.index.diff("HEAD")
            for item in staged_files:
                status['staged'].append(item.a_path)
            
            # Get unstaged files
            unstaged_files = self.repo.index.diff(None)
            for item in unstaged_files:
                status['unstaged'].append(item.a_path)
            
            # Get untracked files
            status['untracked'] = self.repo.untracked_files
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to get git status: {e}")
            return {'error': [str(e)]}
    
    def rollback_changes(self, files: List[str]) -> Tuple[bool, str]:
        if not self.repo:
            return False, "Git repository not initialized"
        
        try:
            # Remove files from staging if they're staged
            for file_path in files:
                if file_path in [item.a_path for item in self.repo.index.diff("HEAD")]:
                    self.repo.index.reset([file_path])
            
            # Delete the actual files
            for file_path in files:
                full_path = self.repo_path / file_path
                if full_path.exists():
                    full_path.unlink()
                    logger.info(f"Deleted file: {full_path}")
            
            return True, f"Successfully rolled back {len(files)} files"
            
        except Exception as e:
            logger.error(f"Failed to rollback changes: {e}")
            return False, f"Failed to rollback changes: {e}"