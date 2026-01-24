from utils import examples_to_str, read_json, write_json
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union


class MSchema:
    def __init__(self, db_id: str = 'Anonymous', schema: Optional[str] = None):
        self.db_id = db_id
        self.schema = schema
        self.tables = {}
        self.foreign_keys = []

    def add_table(self, name, fields={}, comment=None):
        self.tables[name] = {"fields": fields.copy(), 'examples': [], 'comment': comment}

    def add_field(self, table_name: str, field_name: str, field_type: str = "",
            primary_key: bool = False, nullable: bool = True, default: Any = None,
            autoincrement: bool = False, comment: str = "", examples: list = [], **kwargs):
        self.tables[table_name]["fields"][field_name] = {
            "type": field_type,
            "primary_key": primary_key,
            "nullable": nullable,
            "default": default if default is None else f'{default}',
            "autoincrement": autoincrement,
            "comment": comment,
            "examples": examples.copy(),
            **kwargs}

    def add_foreign_key(self, table_name, field_name, ref_schema, ref_table_name, ref_field_name):
        self.foreign_keys.append([table_name, field_name, ref_schema, ref_table_name, ref_field_name])

    def get_field_type(self, field_type, simple_mode=True)->str:
        if not simple_mode:
            return field_type
        else:
            return field_type.split("(")[0]

    def has_table(self, table_name: str) -> bool:
        if table_name in self.tables.keys():
            return True
        else:
            return False

    def has_column(self, table_name: str, field_name: str) -> bool:
        if self.has_table(table_name):
            if field_name in self.tables[table_name]["fields"].keys():
                return True
            else:
                return False
        else:
            return False

    def get_field_info(self, table_name: str, field_name: str) -> Dict:
        try:
            return self.tables[table_name]['fields'][field_name]
        except:
            return {}

    def single_table_mschema(self, table_name: str, selected_columns: List = None,
                             example_num=3, show_type_detail=False) -> str:
        table_info = self.tables.get(table_name, {})
        output = []
        table_comment = table_info.get('comment', '')
        if table_comment is not None and table_comment != 'None' and len(table_comment) > 0:
            if self.schema is not None and len(self.schema) > 0:
                output.append(f"# Table: {self.schema}.{table_name}, {table_comment}")
            else:
                output.append(f"# Table: {table_name}, {table_comment}")
        else:
            if self.schema is not None and len(self.schema) > 0:
                output.append(f"# Table: {self.schema}.{table_name}")
            else:
                output.append(f"# Table: {table_name}")

        field_lines = []
        # 处理表中的每一个字段
        for field_name, field_info in table_info['fields'].items():
            if selected_columns is not None and field_name.lower() not in selected_columns:
                continue

            raw_type = self.get_field_type(field_info['type'], not show_type_detail)
            field_line = f"({field_name}:{raw_type.upper()}"
            if field_info['comment'] != '':
                field_line += f", {field_info['comment'].strip()}"
            else:
                pass

            ## 打上主键标识
            is_primary_key = field_info.get('primary_key', False)
            if is_primary_key:
                field_line += f", Primary Key"

            # 如果有示例，添加上
            if len(field_info.get('examples', [])) > 0 and example_num > 0:
                examples = field_info['examples']
                examples = [s for s in examples if s is not None]
                examples = examples_to_str(examples)
                if len(examples) > example_num:
                    examples = examples[:example_num]

                if raw_type in ['DATE', 'TIME', 'DATETIME', 'TIMESTAMP']:
                    examples = [examples[0]]
                elif len(examples) > 0 and max([len(s) for s in examples]) > 20:
                    if max([len(s) for s in examples]) > 50:
                        examples = []
                    else:
                        examples = [examples[0]]
                else:
                    pass
                if len(examples) > 0:
                    example_str = ', '.join([str(example) for example in examples])
                    field_line += f", Examples: [{example_str}]"
                else:
                    pass
            else:
                field_line += ""
            field_line += ")"

            field_lines.append(field_line)
        output.append('[')
        output.append(',\n'.join(field_lines))
        output.append(']')

        return '\n'.join(output)

    def to_mschema(self, selected_tables: List = None, selected_columns: List = None,
                   example_num=3, show_type_detail=False) -> str:
        """
        convert to a MSchema string.
        selected_tables: 默认为None，表示选择所有的表
        selected_columns: 默认为None，表示所有列全选，格式['table_name.column_name']
        """
        output = []

        output.append(f"【DB_ID】 {self.db_id}")
        output.append(f"【Schema】")

        if selected_tables is not None:
            selected_tables = [s.lower() for s in selected_tables]
        if selected_columns is not None:
            selected_columns = [s.lower() for s in selected_columns]
            selected_tables = [s.split('.')[0].lower() for s in selected_columns]

        # 依次处理每一个表
        for table_name, table_info in self.tables.items():
            if selected_tables is None or table_name.lower() in selected_tables:
                cur_table_type = table_info.get('type', 'table')
                column_names = list(table_info['fields'].keys())
                if selected_columns is not None:
                    cur_selected_columns = [c.lower() for c in column_names if f"{table_name}.{c}".lower() in selected_columns]
                else:
                    cur_selected_columns = selected_columns
                output.append(self.single_table_mschema(table_name, cur_selected_columns, example_num, show_type_detail))

        # 添加外键信息，选择table_type为view时不展示外键
        if self.foreign_keys:
            output.append("【Foreign keys】")
            for fk in self.foreign_keys:
                ref_schema = fk[2]
                table1, column1, _, table2, column2 = fk
                if selected_tables is None or \
                        (table1.lower() in selected_tables and table2.lower() in selected_tables):
                    if ref_schema == self.schema:
                        output.append(f"{fk[0]}.{fk[1]}={fk[3]}.{fk[4]}")

        return '\n'.join(output)

    def to_ddl(self, selected_tables: List = None, example_num=3, max_token=3500) -> str:
        """
        convert to DDL string.
        selected_tables: 默认为None，表示选择所有的表
        max_token: default 3500. 
                   Strategy: 
                   1. Calculate DDL length without examples.
                   2. If > max_token, return without examples.
                   3. Try with 3 examples per column.
                   4. If > max_token, try with 1 example per column.
                   5. If still > max_token, return without examples.
        """
        
        if selected_tables is not None:
            selected_tables = [s.lower() for s in selected_tables]

        def get_estimated_tokens(text):
            return len(text) // 4

        def is_binary_type(field_type):
            type_lower = field_type.lower()
            return any(t in type_lower for t in ['blob', 'binary', 'bytea', 'image', 'varbinary'])

        def format_examples(field_info, num_examples):
            if num_examples <= 0:
                return ""
            
            examples = field_info.get('examples', [])
            if not examples:
                return ""
                
            # Filter and truncate examples
            processed_examples = []
            for ex in examples:
                if ex is None:
                    continue
                s_ex = str(ex)
                if len(s_ex) > 50:
                    s_ex = s_ex[:50] + "..."
                processed_examples.append(s_ex)
            
            # Use utility to sort/filter further if needed, but here we just slice
            if len(processed_examples) > num_examples:
                processed_examples = processed_examples[:num_examples]

            if not processed_examples:
                return ""

            # Check if it looks like a number to decide on quoting
            # Re-using the logic from original code loosely, but simplifying for the new structure
            # Original code check for numeric types to avoid quotes was specific.
            # Here we follow the pattern: quote unless it's strictly numeric and we want to preserve that.
            # But `examples` in field_info seems to come largely as strings or mixed.
            # Let's rely on the simplified formatting:
            
            ex_str_list = []
            raw_type = self.get_field_type(field_info['type'], True).lower()
            is_numeric = 'int' in raw_type or 'real' in raw_type or 'float' in raw_type or 'numeric' in raw_type

            for ex in processed_examples:
                if is_numeric and ex.replace('.', '', 1).isdigit():
                    ex_str_list.append(ex)
                else:
                    ex_str_list.append(f"'{ex}'")
            
            return f", -- example: [{', '.join(ex_str_list)}]"

        def generate_ddl(num_examples_per_field):
            output = []
            for table_name, table_info in self.tables.items():
                if selected_tables is None or table_name.lower() in selected_tables:
                    output.append(f" CREATE TABLE {table_name} (")
                    
                    field_lines = []
                    pks = []
                    
                    for field_name, field_info in table_info['fields'].items():
                        raw_type = self.get_field_type(field_info['type'], True) 
                        line = f" {field_name} {raw_type.lower()}"
                        
                        if field_info.get('primary_key', False):
                            pks.append(field_name)
                        
                        # Add examples if requested and NOT binary
                        if num_examples_per_field > 0 and not is_binary_type(field_info['type']):
                            line += format_examples(field_info, num_examples_per_field)
                        
                        field_lines.append(line)

                    # Primary Keys
                    if pks:
                        field_lines.append(f" PRIMARY KEY ({', '.join(pks)})")
                    
                    # Foreign Keys (Constraints) 
                    for fk in self.foreign_keys:
                        t1, c1, s2, t2, c2 = fk
                        if t1 == table_name:
                             constraint_name = f"fk_{t1.replace('.', '_')}_{c1}"                      
                             field_lines.append(f" CONSTRAINT {constraint_name} FOREIGN KEY ({c1}) REFERENCES {t2} ({c2})")

                    output.append(',\n'.join(field_lines))
                    output.append(" );")
            return '\n'.join(output)

        # 1. Base DDL (0 examples)
        base_ddl = generate_ddl(0)
        base_tokens = get_estimated_tokens(base_ddl)
        
        if base_tokens > max_token:
            return base_ddl
            
        # 3. Try 3 examples
        try_3 = generate_ddl(example_num)
        if get_estimated_tokens(try_3) <= max_token:
            return try_3
        
        # 4. Try 1 example
        if example_num > 1:
            try_1 = generate_ddl(1)
            if get_estimated_tokens(try_1) <= max_token:
                return try_1
        
        # 5. Fallback to base
        return base_ddl

    def dump(self):
        schema_dict = {
            "db_id": self.db_id,
            "schema": self.schema,
            "tables": self.tables,
            "foreign_keys": self.foreign_keys
        }
        return schema_dict

    def save(self, file_path: str):
        schema_dict = self.dump()
        write_json(file_path, schema_dict)

    def load(self, file_path: str):
        data = read_json(file_path)
        self.db_id = data.get("db_id", "Anonymous")
        self.schema = data.get("schema", None)
        self.tables = data.get("tables", {})
        self.foreign_keys = data.get("foreign_keys", [])
