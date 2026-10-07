import inspect
import re
from typing import Type, Dict, Any, List, Optional
from pydantic import BaseModel
from pydantic_core import PydanticUndefined
from path_set import LocalPath
from tqdm import tqdm
from pathlib import Path

doc_path = LocalPath.workspace_operator_docs_root()
builtin_doc_path = LocalPath.workspace_builtin_ops_doc_root()
deduplicator_doc_path = LocalPath.workspace_deduplicator_ops_doc_root()
filter_doc_path = LocalPath.workspace_filter_ops_doc_root()
mapper_doc_path = LocalPath.workspace_mapper_ops_doc_root()
other_operator_doc_path = LocalPath.workspace_other_operator_doc_root()
reader_doc_path = LocalPath.workspace_reader_ops_doc_root()
reducer_doc_path = LocalPath.workspace_reducer_ops_doc_root()
sampler_doc_path = LocalPath.workspace_sampler_ops_doc_root()
writer_doc_path = LocalPath.workspace_writer_ops_doc_root()


def extract_class_doc(operator_cls: Type) -> str:
    return (
        operator_cls.__doc__.strip().replace("\n", "<br>")
        if operator_cls.__doc__
        else ""
    )


def extract_class_name(operator_cls: Type) -> str:
    return operator_cls.__name__


def extract_module_path(operator_cls: Type) -> str:
    return operator_cls.__module__


def extract_direct_inheritance(operator_cls: Type) -> Optional[Type]:
    cls_tuple = inspect.getmro(operator_cls)
    return cls_tuple[1] if len(cls_tuple) > 2 else None


def transfer_cls2module_name(operator_cls_name: Type) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", operator_cls_name)
    # 2. 在大写字母与大写字母之间插入下划线 (针对 XMLParser 这种连续大写)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    # 3. 统一转为小写
    return s2.lower()


def extract_params(operator_cls: Type) -> Dict[str, List[Dict[str, Any]]]:
    """
    遍历算子类的继承链，提取所有包含 'Params' 的内置类参数元数据。

    :param operator_cls: 算子类本身（例如 Operator）
    :return: 字典，key 为找到的 Params 类路径，value 为该类下的参数列表
    """
    result = {}
    res_string = """| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |\n|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|\n"""
    # inspect.getmro() 可以获取类及其所有父类的继承顺序列表（从子类到父类）
    param_list = []
    for cls in inspect.getmro(operator_cls):
        if cls is object:
            continue

        for name, member in cls.__dict__.items():
            if (
                "Params" in name
                and inspect.isclass(member)
                and issubclass(member, BaseModel)
            ):
                class_path = f"{cls.__name__}.{name}"

                if class_path in result:
                    continue

                for field_name, field_info in member.model_fields.items():
                    description = field_info.description or "无描述"

                    if field_info.default is PydanticUndefined:
                        is_required = True
                        default_value = None
                    else:
                        is_required = False
                        default_value = field_info.default

                    raw_annotation = field_info.annotation
                    if raw_annotation is not None:
                        type_str = str(raw_annotation)
                    else:
                        type_str = "None"
                    type_str = type_str.replace("typing.", "").replace(
                        "NoneType", "None"
                    )
                    extra = field_info.json_schema_extra or {}

                    param_list.append(
                        {
                            "param_name": field_name,
                            "type": type_str,
                            "default": default_value,
                            "required": is_required,
                            "description": description,
                            "options": extra.get("options"),
                        }
                    )

    for param in param_list:
        options = param["options"]
        options_string = ""
        if options:
            for k, v in options.items():
                options_string += f"- `{k}`<br/>{v}<br/>".strip()
        param_desc = param["description"].replace("\n", "<br>")
        param_string = f"|`{param['param_name']}`|`{param['type']}`|`{param['default']}`|`{param['required']}`|{param_desc}|{options_string}|\n"
        res_string += param_string

    return res_string


def get_processing_op_doc(operator_cls: Type) -> str:
    title_string = f"# `{extract_class_name(operator_cls)}` Operator\n\n---"
    desc_string = extract_class_doc(operator_cls)
    module_path = extract_module_path(operator_cls)
    module_string = f"- Module path 模块路径: `{module_path}`"
    cls_name = extract_class_name(operator_cls)
    cls_string = f"- Class name 类名: `{cls_name}`"
    inheritance_cls = extract_direct_inheritance(operator_cls)
    inheritance_string = (
        f"- Inherit from 继承于: [{inheritance_cls.__name__}](../meta_operator/{transfer_cls2module_name(inheritance_cls.__name__)}.md)"
        if inheritance_cls
        else "- Inherit from 继承于: `None`"
    )
    test_code_path = f"../../tests/ops/{module_path.split('.')[-2]}/test_{transfer_cls2module_name(operator_cls.__name__)}.py"
    test_code_path_string = f"- Test code 测试代码: [test code]({test_code_path})"
    operator_type = operator_cls.__operator_type__
    if operator_type == "META":
        operator_type_string = f"- Operator type 算子类型: `meta operator`"
        pipeline_applicability_string = f"- Pipeline applicability 流水线适用性: `No`"
    else:
        operator_type_string = f"- Operator type 算子类型: `processing operator`"
        pipeline_applicability_string = f"- Pipeline applicability 流水线适用性: `Yes`"
    try:
        example = operator_cls.EXAMPLE
    except:
        example = ""
    try:
        constraint = operator_cls.CONSTRAINT
    except:
        constraint = ""

    example_string = (
        f"- Example 示例: \n```yaml\n{example}\n```"
        if example
        else "- Example 示例: `None`"
    )
    params_table_string = extract_params(operator_cls)
    params_string = (
        f"## Specific Parameters 具体参数 \n\n{params_table_string}"
        if params_table_string
        else "## Specific Parameters 具体参数 \n\n `None`"
    )
    constraint_string = f"## Constraint 约束\n{constraint}"
    back_string = f"Back to [operator market 算子市场](../ops_market.md)"
    doc = f"""{title_string}\n\n{desc_string}\n\n## Basic Information 基本信息\n{module_string}\n{cls_string}\n{inheritance_string}\n{test_code_path_string}\n{operator_type_string}\n{pipeline_applicability_string}\n{example_string}\n{params_string}\n\n{constraint_string}\n\n🏡 {back_string}\n""".strip()
    return doc


def save_doc(path: Path, doc: str):
    print(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(doc)


def main(processing_operators):

    # region: operator market
    ops_market_doc_string = """#Sub Operator market"""
    table_title = """| Name | Description | Details |\n|:--|:--|:--|\n"""
    builtin_doc_string = """## Built-in\n\n""" + table_title
    reader_doc_string = """## Reader\n\n""" + table_title
    mapper_doc_string = """## Mapper\n\n""" + table_title
    filter_doc_string = """## Filter\n\n""" + table_title
    deduplicator_doc_string = """## Deduplicator\n\n""" + table_title
    reducer_doc_string = """## Reducer\n\n""" + table_title
    sampler_doc_string = """## Sampler\n\n""" + table_title
    writer_doc_string = """## Writer\n\n""" + table_title
    other_operator_doc_string = """## Other\n\n""" + table_title
    for processing_operator in tqdm(
        processing_operators, desc="Check operator market document..."
    ):
        module_path = extract_module_path(processing_operator)
        metaop_belong = module_path.split(".")[-2]
        module_name = module_path.split(".")[-1]
        match metaop_belong:
            case "builtin":
                builtin_doc_string += f"| `{module_name}` | {extract_class_doc(processing_operator)} | [{module_name}]({metaop_belong}/{module_name}.md) |\n"
            case "reader":
                reader_doc_string += f"| `{module_name}` | {extract_class_doc(processing_operator)} | [{module_name}]({metaop_belong}/{module_name}.md) |\n"
            case "mapper":
                mapper_doc_string += f"| `{module_name}` | {extract_class_doc(processing_operator)} | [{module_name}]({metaop_belong}/{module_name}.md) |\n"
            case "filter":
                filter_doc_string += f"| `{module_name}` | {extract_class_doc(processing_operator)} | [{module_name}]({metaop_belong}/{module_name}.md) |\n"
            case "deduplicator":
                deduplicator_doc_string += f"| `{module_name}` | {extract_class_doc(processing_operator)} | [{module_name}]({metaop_belong}/{module_name}.md) |\n"
            case "reducer":
                reducer_doc_string += f"| `{module_name}` | {extract_class_doc(processing_operator)} | [{module_name}]({metaop_belong}/{module_name}.md) |\n"
            case "sampler":
                sampler_doc_string += f"| `{module_name}` | {extract_class_doc(processing_operator)} | [{module_name}]({metaop_belong}/{module_name}.md) |\n"
            case "writer":
                writer_doc_string += f"| `{module_name}` | {extract_class_doc(processing_operator)} | [{module_name}]({metaop_belong}/{module_name}.md) |\n"
            case "other":
                other_operator_doc_string += f"| `{module_name}` | {extract_class_doc(processing_operator)} | [{module_name}]({metaop_belong}/{module_name}.md) |\n"
    ops_market_doc_string += (
        builtin_doc_string
        + "\n\n"
        + reader_doc_string
        + "\n\n"
        + mapper_doc_string
        + "\n\n"
        + filter_doc_string
        + "\n\n"
        + deduplicator_doc_string
        + "\n\n"
        + reducer_doc_string
        + "\n\n"
        + sampler_doc_string
        + "\n\n"
        + writer_doc_string
        + "\n\n"
        + other_operator_doc_string
    )
    save_doc(doc_path.joinpath("ops_market.md"), ops_market_doc_string)
    # endregion

    # region: processing operator docs
    for processing_operator in tqdm(
        processing_operators, desc="Check processing operator"
    ):
        file_name = f"{extract_module_path(processing_operator).split('.')[-1]}.md"
        module_name = processing_operator.__module__.split(".")[-2]
        processing_operator_doc_path = None
        match module_name:
            case "builtin":
                processing_operator_doc_path = builtin_doc_path
            case "deduplicator":
                processing_operator_doc_path = deduplicator_doc_path
            case "filter":
                processing_operator_doc_path = filter_doc_path
            case "mapper":
                processing_operator_doc_path = mapper_doc_path
            case "other":
                processing_operator_doc_path = other_operator_doc_path
            case "reader":
                processing_operator_doc_path = reader_doc_path
            case "reducer":
                processing_operator_doc_path = reducer_doc_path
            case "sampler":
                processing_operator_doc_path = sampler_doc_path
            case "writer":
                processing_operator_doc_path = writer_doc_path
        save_doc(
            processing_operator_doc_path.joinpath(file_name),
            get_processing_op_doc(processing_operator),
        )
    # endregion

def run():
    from workspace.ops.ops_market import OPS_SET
    main(OPS_SET)
