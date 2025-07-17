import openai
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json
from loguru import logger

from .database_connector import ColumnInfo, TableInfo


@dataclass
class DataTypeSuggestion:
    suggested_type: str
    confidence: float
    reasoning: str
    alternatives: List[str]


class AIAssistant:
    def __init__(self, api_key: str, base_url: str = "https://api.openai.com/v1", 
                 model: str = "gpt-3.5-turbo"):
        self.client = openai.OpenAI(
            api_key=api_key,
            base_url=base_url
        )
        self.model = model
        self.data_type_mapping_cache = {}
    
    def suggest_data_type_mapping(self, oracle_type: str, context: Dict[str, any]) -> DataTypeSuggestion:
        cache_key = f"{oracle_type}_{hash(str(context))}"
        
        if cache_key in self.data_type_mapping_cache:
            return self.data_type_mapping_cache[cache_key]
        
        try:
            prompt = self._build_data_type_mapping_prompt(oracle_type, context)
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a database migration expert specializing in Oracle to SQL Server data type mapping."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            suggestion = self._parse_data_type_response(response.choices[0].message.content)
            self.data_type_mapping_cache[cache_key] = suggestion
            
            logger.info(f"AI suggested data type mapping: {oracle_type} -> {suggestion.suggested_type}")
            return suggestion
            
        except Exception as e:
            logger.error(f"AI data type mapping failed: {e}")
            return DataTypeSuggestion(
                suggested_type="NVARCHAR(255)",
                confidence=0.0,
                reasoning=f"AI mapping failed: {e}. Using default safe mapping.",
                alternatives=["NVARCHAR(MAX)", "NTEXT"]
            )
    
    def _build_data_type_mapping_prompt(self, oracle_type: str, context: Dict[str, any]) -> str:
        return f"""
I need to map an Oracle data type to SQL Server. Please provide the best SQL Server equivalent.

Oracle Data Type: {oracle_type}
Context:
- Column Name: {context.get('column_name', 'N/A')}
- Max Length: {context.get('max_length', 'N/A')}
- Precision: {context.get('precision', 'N/A')}
- Scale: {context.get('scale', 'N/A')}
- Nullable: {context.get('nullable', 'N/A')}
- Table Name: {context.get('table_name', 'N/A')}
- Sample Values: {context.get('sample_values', 'N/A')}

Please respond with a JSON object containing:
{{
    "suggested_type": "SQL Server data type with length/precision if needed",
    "confidence": 0.95,
    "reasoning": "Brief explanation of why this mapping is recommended",
    "alternatives": ["alternative1", "alternative2"]
}}

Consider:
1. Performance implications
2. Data integrity
3. Storage efficiency
4. SQL Server best practices
5. Potential data loss or truncation
"""
    
    def _parse_data_type_response(self, response: str) -> DataTypeSuggestion:
        try:
            # Extract JSON from response
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            json_str = response[start_idx:end_idx]
            
            data = json.loads(json_str)
            
            return DataTypeSuggestion(
                suggested_type=data.get('suggested_type', 'NVARCHAR(255)'),
                confidence=float(data.get('confidence', 0.5)),
                reasoning=data.get('reasoning', 'No reasoning provided'),
                alternatives=data.get('alternatives', [])
            )
            
        except Exception as e:
            logger.error(f"Failed to parse AI response: {e}")
            return DataTypeSuggestion(
                suggested_type="NVARCHAR(255)",
                confidence=0.0,
                reasoning=f"Failed to parse AI response: {e}",
                alternatives=[]
            )
    
    def validate_naming_convention(self, table_name: str, context: Dict[str, any]) -> Tuple[bool, str, List[str]]:
        try:
            prompt = f"""
I need to validate a SQL Server table name and suggest improvements if needed.

Table Name: {table_name}
Context:
- Source System: {context.get('source_system', 'N/A')}
- Source Schema: {context.get('source_schema', 'N/A')}
- Target Schema: {context.get('target_schema', 'N/A')}
- Naming Convention: {context.get('naming_convention', 'N/A')}

Please validate the table name and respond with JSON:
{{
    "is_valid": true/false,
    "message": "Validation message",
    "suggestions": ["suggestion1", "suggestion2"]
}}

Check for:
1. SQL Server naming rules
2. Reserved words
3. Best practices
4. Consistency with provided naming convention
"""
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a database naming expert for SQL Server."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=300
            )
            
            result = self._parse_validation_response(response.choices[0].message.content)
            return result
            
        except Exception as e:
            logger.error(f"AI naming validation failed: {e}")
            return False, f"AI validation failed: {e}", []
    
    def _parse_validation_response(self, response: str) -> Tuple[bool, str, List[str]]:
        try:
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            json_str = response[start_idx:end_idx]
            
            data = json.loads(json_str)
            
            return (
                data.get('is_valid', False),
                data.get('message', 'No message provided'),
                data.get('suggestions', [])
            )
            
        except Exception as e:
            logger.error(f"Failed to parse validation response: {e}")
            return False, f"Failed to parse validation response: {e}", []
    
    def generate_documentation(self, table_info: TableInfo) -> str:
        try:
            prompt = f"""
Generate documentation for a migrated database table.

Table Information:
- Name: {table_info.table_name}
- Schema: {table_info.schema_name}
- Type: {table_info.table_type}
- Columns: {len(table_info.columns)}
- Row Count: {table_info.row_count or 'Unknown'}

Column Details:
{self._format_columns_for_prompt(table_info.columns)}

Please generate comprehensive documentation including:
1. Table purpose and description
2. Column descriptions
3. Data types and constraints
4. Performance considerations
5. Migration notes

Format as Markdown.
"""
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a technical documentation expert for database migrations."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1000
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"AI documentation generation failed: {e}")
            return f"# {table_info.table_name}\n\nDocumentation generation failed: {e}"
    
    def _format_columns_for_prompt(self, columns: List[ColumnInfo]) -> str:
        formatted = []
        for col in columns:
            formatted.append(f"- {col.column_name}: {col.data_type} "
                           f"({'NULL' if col.nullable else 'NOT NULL'}) "
                           f"{'PK' if col.is_primary_key else ''}")
        return "\n".join(formatted)
    
    def suggest_performance_optimizations(self, table_info: TableInfo) -> List[str]:
        try:
            prompt = f"""
Analyze this SQL Server table and suggest performance optimizations.

Table: {table_info.table_name}
Columns: {len(table_info.columns)}
Estimated Rows: {table_info.row_count or 'Unknown'}

Column Analysis:
{self._format_columns_for_prompt(table_info.columns)}

Primary Keys: {', '.join(table_info.primary_keys) if table_info.primary_keys else 'None'}

Please provide specific optimization recommendations focusing on:
1. Indexing strategy
2. Data type optimization
3. Storage considerations
4. Query performance
5. Maintenance recommendations

Respond with a JSON array of recommendations:
["recommendation1", "recommendation2", ...]
"""
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a SQL Server performance optimization expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=800
            )
            
            return self._parse_recommendations_response(response.choices[0].message.content)
            
        except Exception as e:
            logger.error(f"AI performance optimization failed: {e}")
            return [f"Performance optimization analysis failed: {e}"]
    
    def _parse_recommendations_response(self, response: str) -> List[str]:
        try:
            # Try to extract JSON array from response
            start_idx = response.find('[')
            end_idx = response.rfind(']') + 1
            
            if start_idx >= 0 and end_idx > start_idx:
                json_str = response[start_idx:end_idx]
                recommendations = json.loads(json_str)
                return recommendations
            else:
                # Fallback: split by lines and clean up
                lines = response.split('\n')
                recommendations = []
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith('{') and not line.startswith('}'):
                        # Remove common prefixes
                        line = line.lstrip('- *•').strip()
                        if line:
                            recommendations.append(line)
                return recommendations[:10]  # Limit to 10 recommendations
                
        except Exception as e:
            logger.error(f"Failed to parse recommendations response: {e}")
            return ["Failed to parse performance recommendations"]
    
    def explain_data_type_mapping(self, oracle_type: str, sql_server_type: str) -> str:
        try:
            prompt = f"""
Explain the data type mapping from Oracle to SQL Server for end users.

Oracle Type: {oracle_type}
SQL Server Type: {sql_server_type}

Please provide a clear, non-technical explanation covering:
1. What this mapping means
2. Any potential data considerations
3. Performance implications
4. Best practices for using this data type

Keep the explanation user-friendly and practical.
"""
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a database expert who explains technical concepts in simple terms."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=400
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"AI explanation failed: {e}")
            return f"Unable to generate explanation: {e}"
    
    def suggest_migration_strategy(self, table_info: TableInfo, target_environment: str) -> Dict[str, any]:
        try:
            prompt = f"""
Suggest a migration strategy for this table.

Table: {table_info.schema_name}.{table_info.table_name}
Type: {table_info.table_type}
Estimated Rows: {table_info.row_count or 'Unknown'}
Target Environment: {target_environment}

Column Summary:
- Total Columns: {len(table_info.columns)}
- Primary Keys: {len(table_info.primary_keys)}
- Large Object Columns: {len([c for c in table_info.columns if 'MAX' in c.data_type])}

Please provide a migration strategy as JSON:
{{
    "approach": "full/incremental/batch",
    "estimated_duration": "time estimate",
    "risk_level": "low/medium/high",
    "recommendations": ["rec1", "rec2"],
    "prerequisites": ["prereq1", "prereq2"],
    "post_migration_tasks": ["task1", "task2"]
}}
"""
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a database migration strategy expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=600
            )
            
            return self._parse_migration_strategy_response(response.choices[0].message.content)
            
        except Exception as e:
            logger.error(f"AI migration strategy failed: {e}")
            return {
                "approach": "unknown",
                "estimated_duration": "unknown",
                "risk_level": "unknown",
                "recommendations": [f"Strategy generation failed: {e}"],
                "prerequisites": [],
                "post_migration_tasks": []
            }
    
    def _parse_migration_strategy_response(self, response: str) -> Dict[str, any]:
        try:
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            json_str = response[start_idx:end_idx]
            
            return json.loads(json_str)
            
        except Exception as e:
            logger.error(f"Failed to parse migration strategy response: {e}")
            return {
                "approach": "unknown",
                "estimated_duration": "unknown",
                "risk_level": "unknown",
                "recommendations": ["Failed to parse strategy response"],
                "prerequisites": [],
                "post_migration_tasks": []
            }