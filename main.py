import os
from datetime import datetime, date
import re
import json
import io
import time
import random
import tempfile
import base64
import pandas as pd

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

import boto3
from botocore.exceptions import ClientError
import streamlit as st

# ======================= UI ЧАСТЬ =========================
st.set_page_config(page_title="S3 File Uploader", layout="centered")

st.write("")
st.title("Предоставление отсрочки по БЗК")
st.write("Причина: Выход в отпуск по уходу за ребенком (декрет)")

# --- Основные параметры ---
AWS_PROFILE = ""   # профиль AWS из ~/.aws/credentials (оставьте пустым для env/role)
AWS_REGION = "us-east-1"   # регион AWS
BEDROCK_REGION = "us-east-1"  # регион Bedrock
MODEL_ID = "anthropic.claude-3-7-sonnet-20250219-v1:0"  # используемая LLM модель с vision
BUCKET_NAME = "loan-deferment-idp-test-tlek"  # имя S3-бакета
KEY_PREFIX = "uploads/"  # базовый префикс для загрузок

# Inference Profile for Claude 3.7 Sonnet (can be ID or ARN). ARN is recommended.
DEFAULT_INFERENCE_PROFILE_ID = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
DEFAULT_INFERENCE_PROFILE_ARN = "arn:aws:bedrock:us-east-1:183295407481:inference-profile/us.anthropic.claude-3-7-sonnet-20250219-v1:0"


# ======================= КОНСТАНТЫ И УТИЛИТЫ =========================
# Варианты типа документа (отображаемые метки)
DOC_TYPE_OPTIONS = [
    "Лист временной нетрудоспособности (больничный лист)",
    "Приказ о выходе в декретный отпуск по уходу за ребенком",
    "Справка о выходе в декретный отпуск по уходу за ребенком",
]

# Маппинг из меток UI к коротким значениям
DOC_TYPE_VALUE_MAP = {
    "Лист временной нетрудоспособности (больничный лист)": "Лист",
    "Приказ о выходе в декретный отпуск по уходу за ребенком": "Приказ",
    "Справка о выходе в декретный отпуск по уходу за ребенком": "Справка",
}

# Сообщения верификации МИБ (успешные тексты для зелёных статусов)
MIB_RULES = {
    "ФИО заявителя и ФИО в документе должны совпадать": {
        "success": "ФИО совпадает.",
    },
    "Наименование документа": {
        "success": "Тип документа подтверждён.",
    },
    "Актуальная дата": {
        "success": "Документ в пределах срока актуальности.",
    },
    "Наличие QR или печати": {
        "success": "Обнаружены печать и/или QR.",
    },
    "Прикрепленный файл должен содержать один документ": {
        "success": "Загружен один документ (1 страница PDF).",
    },
}

# Сроки актуальности по типу документа (календарные дни)
VALIDITY_DAYS = {"Лист": 180, "Приказ": 30, "Справка": 10}

def norm_name(val: str | None) -> str | None:
    """Нормализация ФИО: тримминг, нижний регистр, удаление лишних символов."""
    if not isinstance(val, str) or not val.strip():
        return None
    s = re.sub(r"\s+", " ", val.strip()).lower()
    s = re.sub(r"[^a-zа-яё\s-]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s

def format_date_ddmmyyyy(val) -> str:
    """Единое форматирование даты для отображения: DD/MM/YYYY. Принимает str | datetime | date | None."""
    d: date | None = None
    if isinstance(val, str):
        d = parse_date_safe(val)
    elif isinstance(val, datetime):
        d = val.date()
    elif isinstance(val, date):
        d = val
    if d is None:
        return "—"
    return d.strftime("%d/%m/%Y")

# Сообщения и коды ошибок МИБ (для отображения/интеграции)
MIB_ERRORS = {
    # Ключи соответствуют заголовкам проверок/полей в UI
    "Наименование документа": {
        "message": "Не верный формат документа. Пожалуйста, проверьте правильность выбранных данных",
        "code": "01",
    },
    "Актуальная дата": {
        "message": "Не верный формат документа. Загрузите пожалуйста обновленный документ с актуальной датой. Пожалуйста проверьте правильность выбранных данных",
        "code": "03",
    },
    "Прикрепленный файл должен содержать один документ": {
        "message": "Не верный формат документа. Пожалуйста прикрепите только один документ в одном файле",
        "code": "04",
    },
    "ФИО заявителя и ФИО в документе должны совпадать": {
        "message": "Не верный формат документа. Некоторые документы не относятся к заявителю. Пожалуйста проверьте правильность выбранных данных.",
        "code": "05",
    },
    "Наличие QR или печати": {
        "message": "Не верный формат документа. Некоторые документы не содержат в себе печать/QR подтверждения. Пожалуйста проверьте правильность выбранных данных",
        "code": "06",
    },
}

def norm_doc_type(val: str | None) -> str | None:
    """Приведение типа документа к одному из значений: Лист | Приказ | Справка."""
    if not isinstance(val, str) or not val.strip():
        return None
    s = val.strip().lower()
    if "лист" in s:
        return "Лист"
    if "приказ" in s:
        return "Приказ"
    if "справк" in s:
        return "Справка"
    if s in ("лист", "приказ", "справка"):
        return s.capitalize()
    return None


def parse_date_safe(s: str | None):
    """Пробуем распарсить дату в нескольких популярных форматах. Возвращает date или None."""
    if not isinstance(s, str) or not s.strip():
        return None
    s = s.strip()
    fmts = ["%d.%m.%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def parse_json_relaxed(s: str) -> dict | None:
    """Пытаемся распарсить JSON. Если не получается, вырезаем фрагмент между первой '{' и последней '}'."""
    try:
        return json.loads(s)
    except Exception:
        start = s.find("{")
        end = s.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(s[start:end + 1])
            except Exception:
                return None
        return None

def render_detailed_checks(parsed: dict):
    """Рендерит детальные проверки во вкладке 'Детальная проверка'."""
    # Соответствие ФИО
    st.markdown("#### Соответствие ФИО")
    client_fio_raw = st.session_state.get("client_fio")
    bedrock_fio_raw = parsed.get("ФИО заявителя")
    client_fio = norm_name(client_fio_raw)
    bedrock_fio = norm_name(bedrock_fio_raw)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**ФИО заявителя:**")
        st.write(client_fio_raw if client_fio_raw else "—")
    with col2:
        st.markdown("**ФИО в документе:**")
        st.write(bedrock_fio_raw if bedrock_fio_raw else "—")
    if client_fio is None and bedrock_fio is None:
        st.info("Недостаточно данных для проверки ФИО.")
    elif client_fio is None or bedrock_fio is None:
        st.warning("Одно из значений ФИО отсутствует — невозможно проверить совпадение.")
    else:
        if client_fio == bedrock_fio:
            ok = (MIB_RULES.get("ФИО заявителя и ФИО в документе должны совпадать") or {}).get("success")
            st.success(ok or "ФИО совпадает.")
        else:
            err = MIB_ERRORS.get("ФИО заявителя и ФИО в документе должны совпадать")
            if err:
                st.error(f"Код Ошибки {err['code']}: {err['message']}")
            else:
                st.error("Ошибка верификации ФИО")

    st.divider()
    st.markdown("#### Соответствие типа документа")
    client_doc_value = (parsed.get("_client", {}) or {}).get("doc_type_value")
    bedrock_doc_value_raw = parsed.get("Тип документа")
    client_doc_norm = norm_doc_type(client_doc_value)
    bedrock_doc_norm = norm_doc_type(bedrock_doc_value_raw)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Тип документа выбранного клиентом:**")
        st.write(f"{client_doc_norm if client_doc_norm else '—'}")
    with c2:
        st.markdown("**Тип загруженного документа:**")
        st.write(f"{bedrock_doc_norm if bedrock_doc_norm else '—'}")
    if client_doc_norm is None and bedrock_doc_norm is None:
        st.info("Недостаточно данных для проверки типа документа.")
    elif client_doc_norm is None or bedrock_doc_norm is None:
        st.warning("Одно из значений типа документа отсутствует — невозможно проверить совпадение.")
    else:
        if client_doc_norm == bedrock_doc_norm:
            ok = (MIB_RULES.get("Наименование документа") or {}).get("success")
            st.success(ok or "Тип документа подтверждён.")
        else:
            err = MIB_ERRORS.get("Наименование документа")
            if err:
                st.error(f"Код Ошибки {err['code']}: {err['message']}")
            else:
                st.error("Ошибка верификации типа документа")

    st.divider()
    st.markdown("#### Актуальность документа")
    # Текущая дата
    today = datetime.utcnow().date()
    # Срок актуальности по типу
    doc_type_for_validity = bedrock_doc_norm or client_doc_norm
    validity_days = VALIDITY_DAYS.get(doc_type_for_validity) if doc_type_for_validity else None
    issue_date_raw = parsed.get("Дата выдачи документа")
    issue_date = parse_date_safe(issue_date_raw)
    expires_date = None
    if isinstance(validity_days, int) and issue_date is not None:
        expires = issue_date.toordinal() + validity_days
        expires_date = datetime.fromordinal(expires).date()
    validity_rows = [
        {"Поле": "Актуальная дата", "Значение": format_date_ddmmyyyy(today)},
        {"Поле": "Срок актуальности", "Значение": (f"{doc_type_for_validity} {validity_days} кал. дней" if validity_days is not None and doc_type_for_validity else "—")},
        {"Поле": "Дата выдачи документа", "Значение": format_date_ddmmyyyy(issue_date_raw)},
        {"Поле": "Действителен до (включительно)", "Значение": (format_date_ddmmyyyy(expires_date) if expires_date else "—")},
    ]
    _df_validity = pd.DataFrame(validity_rows)
    try:
        st.table(_df_validity.style.hide(axis="index"))
    except Exception:
        st.table(_df_validity.reset_index(drop=True))
    if validity_days is None:
        st.info("Тип документа неизвестен — невозможно оценить актуальность.")
    elif issue_date is None:
        st.warning("Не удалось распознать дату выдачи — невозможно оценить актуальность.")
    elif expires_date is not None:
        if today <= expires_date:
            ok = (MIB_RULES.get("Актуальная дата") or {}).get("success")
            st.success(ok or "Документ в пределах срока актуальности.")
        else:
            err = MIB_ERRORS.get("Актуальная дата")
            if err:
                st.error(f"Код Ошибки {err['code']}: {err['message']}")
            else:
                st.error("Документ не актуален")

    st.divider()
    st.markdown("#### Наличие печати или QR")
    si = parsed.get("_stamps") if isinstance(parsed.get("_stamps"), dict) else {}
    sp = si.get("stamp_present")
    sc = si.get("stamp_confidence")
    qp = si.get("qr_present")
    qc = si.get("qr_confidence")
    if sp is True or qp is True:
        ok = (MIB_RULES.get("Наличие QR или печати") or {}).get("success")
        st.success(ok or "Обнаружены печать и/или QR.")
    elif sp is False and qp is False:
        err = MIB_ERRORS.get("Наличие QR или печати")
        if err:
            st.error(f"Код Ошибки {err['code']}: {err['message']}")
        else:
            st.error("Не обнаружены ни печать, ни QR")
    else:
        st.info("Недостаточно данных для определения наличия печати/QR.")
    colp, colq = st.columns(2)
    with colp:
        st.markdown("**Печать:**")
        if sp is True:
            msg = "обнаружена"
            if isinstance(sc, (int, float)):
                msg += f" (CR {round(sc)}%)"
            st.write(msg)
        elif sp is False:
            st.write("не обнаружена")
        else:
            st.write("не определено")
    with colq:
        st.markdown("**QR-код:**")
        if qp is True:
            msg = "обнаружен"
            if isinstance(qc, (int, float)):
                msg += f" (CR {round(qc)}%)"
            st.write(msg)
        elif qp is False:
            st.write("не обнаружен")
        else:
            st.write("не определено")

    st.divider()
    st.markdown("#### Проверка количества страниц документа")
    _is_pdf_flag = st.session_state.get("last_is_pdf")
    if _is_pdf_flag:
        _pc = st.session_state.get("pdf_page_count")
        if isinstance(_pc, int):
            st.write(f"Страниц в прикрепленном файле: {_pc}")
            if _pc == 1:
                ok = (MIB_RULES.get("Прикрепленный файл должен содержать один документ") or {}).get("success")
                st.success(ok or "Прикрепленный файл содержит один документ")
            elif _pc > 1:
                err = MIB_ERRORS.get("Прикрепленный файл должен содержать один документ")
                if err:
                    st.error(f"Код Ошибки {err['code']}: {err['message']}")
                else:
                    st.error("Прикрепленный файл содержит более одного документа")
            else:
                st.info("Не удалось определить количество страниц в прикрепленном файле.")
        else:
            st.info("Количество страниц в прикрепленном файле неизвестно.")
    else:
        st.caption("Проверка применяется только к PDF-файлам. Для изображений (JPG/JPEG) не выполняется.")

# --- Кастомизация интерфейса ---\
st.markdown("""
<style>
.block-container{max-width:980px;padding-top:1.25rem;}
.meta{color:#6b7280;font-size:0.92rem;margin:0.25rem 0 1rem 0;}
.meta code{background:#f3f4f6;border:1px solid #e5e7eb;padding:2px 6px;border-radius:6px;}
.card{border:1px solid #e5e7eb;border-radius:14px;background:#ffffff;box-shadow:0 2px 8px rgba(0,0,0,.04);} 
.card.pad{padding:22px;}
.result-card{border:1px solid #e5e7eb;border-radius:14px;padding:16px;background:#fafafa;}
.stButton>button{border-radius:10px;padding:.65rem 1rem;font-weight:600;}
.stDownloadButton>button{border-radius:10px;}
</style>
""", unsafe_allow_html=True)

# with st.expander("Помощь и настройка", expanded=False):
#     tabs = st.tabs(["Запуск приложения", "Создание Access Key", "Окружение"])
#     with tabs[0]:
#         st.markdown("#### 1) Установите AWS CLI v2")
#         st.code('''curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"\nsudo installer -pkg AWSCLIV2.pkg -target /''', language="bash")
#         st.code("aws --version", language="bash")
#         st.markdown("#### 2) Настройте учётные данные")
#         st.code("aws configure", language="bash")
#         st.markdown("#### 3) Запустите приложение")
#         st.code("streamlit run main.py", language="bash")
#     with tabs[1]:
#         st.markdown("### 🔑 Создание Access Key (CLI)")
#         st.markdown("Программные ключи нужны для работы из кода/CLI. Создайте их в AWS IAM.")
#     with tabs[2]:
#         st.markdown("### Окружение")
#         st.markdown(f"- Bucket: `{BUCKET_NAME}`\n- Region: `{AWS_REGION}`\n- Model: `{MODEL_ID}`")
#         # Настройка Inference Profile через UI / ENV
#         default_ip = (
#             os.getenv("BEDROCK_INFERENCE_PROFILE")
#             or DEFAULT_INFERENCE_PROFILE_ARN
#             or DEFAULT_INFERENCE_PROFILE_ID
#         )
#         ip_value = st.text_input(
#             "Inference Profile (ID или ARN для Claude 3.7 Sonnet)",
#             value=st.session_state.get("inference_profile", default_ip),
#             help="Например ID: us.anthropic.claude-3-7-sonnet-20250219-v1:0 или ARN: arn:aws:bedrock:...:inference-profile/us.anthropic.claude-3-7-sonnet-20250219-v1:0",
#         )
#         st.session_state["inference_profile"] = ip_value.strip() if ip_value else ""

# --- Форма загрузки ---
with st.form("upload_form", clear_on_submit=False):
    fio = st.text_input(
        "ФИО заявителя",
        value=st.session_state.get("client_fio", ""),
        help="Введите полностью: Фамилия Имя Отчество"
    )
    # Используем единый источник правды для вариантов и маппинга
    doc_type_options = DOC_TYPE_OPTIONS
    doc_type = st.selectbox(
        "Тип документа",
        options=["Выберите тип документа"] + doc_type_options,
        index=0,
        help="Выберите один документ, который вы предоставляете"
    )
    uploaded_file = st.file_uploader(
        "Выберите документ (1 файл)",
        type=["pdf", "jpg"],
        accept_multiple_files=False,
        help="Поддержка: PDF, JPEG. Разрешена загрузка только одного файла.",
    )
    submitted = st.form_submit_button("Загрузить и обработать", type="primary")


# ===================== ФУНКЦИИ ============================

def get_s3_client(profile, region_name):
    if profile:
        session = boto3.session.Session(profile_name=profile, region_name=region_name or None)
        return session.client("s3")
    return boto3.client("s3", region_name=region_name or None)

def get_next_upload_folder(s3_client, bucket, prefix):
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        existing_max = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []) or []:
                p = cp.get("Prefix", "")
                m = re.search(r"upload_id_(\d{3,})/\Z", p)
                if m:
                    existing_max = max(existing_max, int(m.group(1)))
        next_id = existing_max + 1
        return f"{prefix}upload_id_{next_id:03d}/"
    except Exception:
        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        return f"{prefix}upload_id_{ts}/"

def textract_blocks_to_text(tex_resp: dict) -> str:
    lines = [b.get("Text", "") for b in tex_resp.get("Blocks", []) if b.get("BlockType") == "LINE"]
    return "\n".join([ln for ln in lines if ln])

# --- Обнаружение подписей (Textract SIGNATURES) с backoff ---
def detect_signatures(textract_client, bucket: str, key: str, content_type: str):
    results = []
    try:
        is_pdf = ("pdf" in (content_type or "").lower()) or key.lower().endswith(".pdf")
        if not is_pdf:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket, Key=key)
            img_bytes = obj["Body"].read()
            resp = textract_client.analyze_document(Document={"Bytes": img_bytes}, FeatureTypes=["SIGNATURES"])
            for b in resp.get("Blocks", []) or []:
                if b.get("BlockType") == "SIGNATURE":
                    results.append({"confidence": b.get("Confidence"), "geometry": b.get("Geometry"), "page": b.get("Page")})
        else:
            start = textract_client.start_document_analysis(
                DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
                FeatureTypes=["SIGNATURES"],
            )
            job_id = start["JobId"]
            pages = []

            # Backoff функция с поддержкой пагинации через NextToken
            def get_document_analysis_with_backoff(job_id, next_token=None, max_retries=6):
                retries = 0
                while True:
                    try:
                        params = {"JobId": job_id, "MaxResults": 1000}
                        if next_token:
                            params["NextToken"] = next_token
                        resp = textract_client.get_document_analysis(**params)
                        return resp
                    except ClientError as e:
                        if e.response['Error']['Code'] == "ThrottlingException":
                            wait = (2 ** retries) + random.random()
                            time.sleep(wait)
                            retries += 1
                            if retries > max_retries:
                                raise Exception("Превышено количество попыток из-за ThrottlingException")
                        else:
                            raise

            while True:
                resp = get_document_analysis_with_backoff(job_id, next_token=None)
                status = resp["JobStatus"]
                if status == "SUCCEEDED":
                    pages.append(resp)
                    next_token = resp.get("NextToken")
                    while next_token:
                        nxt = get_document_analysis_with_backoff(job_id, next_token=next_token)
                        pages.append(nxt)
                        next_token = nxt.get("NextToken")
                    break
                elif status == "FAILED":
                    raise Exception("Textract анализ не удался")
                else:
                    time.sleep(2 + random.random())

            for page in pages:
                for b in page.get("Blocks", []) or []:
                    if b.get("BlockType") == "SIGNATURE":
                        results.append({"confidence": b.get("Confidence"), "geometry": b.get("Geometry"), "page": b.get("Page")})

    except Exception as e:
        return {"signatures": [], "error": str(e)}
    return {"signatures": results, "error": None}

def _b64_image_from_bytes(img_bytes: bytes, media_type: str) -> dict:
    return {
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": base64.b64encode(img_bytes).decode("utf-8"),
        },
    }

def detect_stamp_llm(bedrock_client, model_id: str, images: list[dict]):
    """
    images: список элементов content для Anthropic messages API вида
      {"type":"image", "source": {"type":"base64","media_type":"image/png","data":"..."}}
    Возвращает: {"present": bool|None, "confidence": float|None, "reason": str|None, "raw": str, "error": None|str}
    """
    try:
        instruction = (
            "Определи, есть ли на изображении отсканированного документа: "
            "1) печать (штамп: круглая или прямоугольная), "
            "2) QR-код (квадратный матричный код)."
        )
        format_req = (
            "Верни строго JSON без пояснений:\n"
            "{\n"
            "  \"stamp_present\": true|false,\n"
            "  \"stamp_confidence\": number (0..100),\n"
            "  \"qr_present\": true|false,\n"
            "  \"qr_confidence\": number (0..100),\n"
            "}"
        )
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 256,
            "temperature": 0,
            "messages": [
                {
                    "role": "user",
                    "content": ([{"type": "text", "text": instruction + "\n" + format_req}] + images),
                }
            ],
        }
        data = _invoke_with_inference_profile(bedrock_client, body, model_id=model_id)
        text = data.get("content", [{}])[0].get("text", "")
        # Попытка распарсить JSON из ответа
        parsed = None
        try:
            parsed = json.loads(text)
        except Exception:
            s = text.find("{")
            e = text.rfind("}")
            if s != -1 and e != -1 and e > s:
                try:
                    parsed = json.loads(text[s:e+1])
                except Exception:
                    parsed = None
        if not isinstance(parsed, dict):
            return {"stamp_present": None, "stamp_confidence": None, "qr_present": None, "qr_confidence": None, "raw": text, "error": "LLM returned non-JSON"}
        return {
            # Существующие поля (совместимость)
            "stamp_present": parsed.get("stamp_present"),
            "stamp_confidence": parsed.get("stamp_confidence"),
            "qr_present": parsed.get("qr_present"),
            "qr_confidence": parsed.get("qr_confidence"),
            # Технические поля
            "raw": text,
            "error": None,
        }
    except Exception as e:
        return {"present": None, "confidence": None, "reason": None, "raw": "", "error": str(e)}

def convert_pdf_to_images_and_store(s3_client, bucket: str, key: str, max_pages: int = 3, zoom: float = 2.0):
    """
    Конвертация первых max_pages страниц PDF (из S3) в PNG изображения.
    Сохраняет локально в /tmp и загружает в S3 по пути previews/page_XXX.png.

    Возвращает dict: {"local_paths": [..], "s3_keys": [..], "page_count": int, "error": None|str}
    """
    if fitz is None:
        return {"local_paths": [], "s3_keys": [], "error": "PyMuPDF (fitz) не установлен"}
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        pdf_bytes = obj["Body"].read()

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_pages = len(doc)
        pages = min(total_pages, max_pages)
        local_paths = []
        s3_keys = []
        tmp_dir = tempfile.mkdtemp(prefix="pdf_previews_")

        # Префикс для S3 (тот же каталог, что и у исходного файла)
        folder = key.rsplit("/", 1)[0] + "/" if "/" in key else ""
        previews_prefix = f"{folder}previews/"

        mat = fitz.Matrix(zoom, zoom)
        for i in range(pages):
            page = doc.load_page(i)
            pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB, alpha=False)
            local_path = os.path.join(tmp_dir, f"page_{i+1:03d}.png")
            pix.save(local_path)
            local_paths.append(local_path)

            preview_key = f"{previews_prefix}page_{i+1:03d}.png"
            with open(local_path, "rb") as f:
                s3_client.upload_fileobj(
                    Fileobj=f,
                    Bucket=bucket,
                    Key=preview_key,
                    ExtraArgs={"ContentType": "image/png"},
                )
            s3_keys.append(preview_key)

        return {"local_paths": local_paths, "s3_keys": s3_keys, "page_count": total_pages, "error": None}
    except Exception as e:
        return {"local_paths": [], "s3_keys": [], "page_count": 0, "error": str(e)}

def build_prompt_russian(extracted_text: str) -> str:
    instruction = (
        "Извлеки следующую информацию из текста.\n"
        "Верни результат строго в формате JSON:\n"
        "{\n"
        "  \"ФИО заявителя\": string | null,\n"
        "  \"Тип документа\": \"Лист\" | \"Приказ\" | \"Справка\" | null,\n"
        "  \"Наименование документа\": string | null,\n"
        "  \"Дата выдачи документа\": string | null,\n"
        "  \"Дата начала отпуска\": string | null,\n"
        "  \"Дата окончания отпуска\": string | null\n"
        "}\n\n"
        "Правила для определения поля 'Тип документа':\n"
        "- Если 'Наименование документа' содержит 'Лист временной нетрудоспособности', то 'Тип документа' = 'Лист'.\n"
        "- Если 'Наименование документа' содержит 'Приказ', то 'Тип документа' = 'Приказ'.\n"
        "- Если 'Наименование документа' содержит 'Справка', то 'Тип документа' = 'Справка'.\n"
        "- Если невозможно определить, то 'Тип документа' = null.\n\n"
        "Текст для анализа:\n"
    )


    return instruction + extracted_text

def get_bedrock_client(profile: str | None, region_name: str | None):
    if profile:
        session = boto3.session.Session(profile_name=profile, region_name=region_name or None)
        return session.client("bedrock-runtime")
    return boto3.client("bedrock-runtime", region_name=region_name or None)

def _get_inference_profile_from_state() -> str | None:
    # Порядок приоритета: UI state -> ENV -> defaults
    ip = (
        st.session_state.get("inference_profile")
        or os.getenv("BEDROCK_INFERENCE_PROFILE")
        or DEFAULT_INFERENCE_PROFILE_ARN
        or DEFAULT_INFERENCE_PROFILE_ID
    )
    return ip

def _invoke_with_inference_profile(client, body: dict, model_id: str):
    payload = json.dumps(body)
    ip = _get_inference_profile_from_state()
    # В текущей версии SDK профиль передаётся в modelId (ID/ARN профиля),
    # так как параметры inferenceProfileArn/Id не поддерживаются.
    target_model_id = (ip.strip() if ip else model_id)
    resp = client.invoke_model(
        modelId=target_model_id,
        contentType="application/json",
        accept="application/json",
        body=payload,
    )
    return json.loads(resp["body"].read())

def call_bedrock_invoke(model_id: str, prompt: str, client):
    if model_id.startswith("anthropic."):
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
            "temperature": 0,
            "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        }
        data = _invoke_with_inference_profile(client, body, model_id=model_id)
        return data.get("content", [{}])[0].get("text", "")
    else:
        body = {"inputText": prompt, "textGenerationConfig": {"maxTokenCount": 1024, "temperature": 0}}
        data = _invoke_with_inference_profile(client, body, model_id=model_id)
        if "results" in data and data["results"]:
            return data["results"][0].get("outputText", "")
        return json.dumps(data)

# =============== ОСНОВНОЙ ПРОЦЕСС =========================
if submitted:
    if not BUCKET_NAME:
        st.error("S3-бакет не настроен.")
    elif not (fio and fio.strip()):
        st.error("Укажите ФИО заявителя.")
    elif doc_type == "Выберите тип документа":
        st.error("Выберите тип документа.")
    elif not uploaded_file:
        st.error("Не выбран файл для загрузки.")
    else:
        # Сохраним значения формы в сессию
        st.session_state["client_fio"] = fio.strip()
        st.session_state["client_doc_type"] = doc_type
        try:
            s3 = get_s3_client(AWS_PROFILE.strip() or None, AWS_REGION)
            progress = st.progress(0)

            original_name = uploaded_file.name
            base_prefix = (KEY_PREFIX or "").strip() or "uploads/"
            if base_prefix and not base_prefix.endswith("/"):
                base_prefix += "/"

            upload_folder = get_next_upload_folder(s3, BUCKET_NAME, base_prefix)
            key = f"{upload_folder}{original_name}"

            uploaded_file.seek(0)
            content_type = getattr(uploaded_file, "type", None) or "application/octet-stream"
            with st.status("Загрузка файла...", expanded=False) as status:
                s3.upload_fileobj(
                    Fileobj=uploaded_file,
                    Bucket=BUCKET_NAME,
                    Key=key,
                    ExtraArgs={"ContentType": content_type},
                )
                s3_uri = f"s3://{BUCKET_NAME}/{key}"
                status.update(label=f"Файл загружен в {s3_uri}", state="complete")
            progress.progress(30)

            
            # st.success(f"Файл успешно загружен в {s3_uri}")

            st.session_state["last_s3_bucket"] = BUCKET_NAME
            st.session_state["last_s3_key"] = key
            st.session_state["last_s3_uri"] = s3_uri

            try:
                with st.status("Обработка документа...", expanded=False) as status:
                    status.update(label="Извлечение текста через Textract...", state="running")
                    if AWS_PROFILE.strip():
                        session = boto3.session.Session(profile_name=AWS_PROFILE.strip(), region_name=AWS_REGION)
                        textract = session.client("textract")
                    else:
                        textract = boto3.client("textract", region_name=AWS_REGION)

                    # Если загружен PDF, создадим превью изображений и сохраним локально и в S3
                    is_pdf = ("pdf" in (content_type or "").lower()) or key.lower().endswith(".pdf")
                    # Сохраним флаг для вкладки проверки
                    st.session_state["last_is_pdf"] = bool(is_pdf)
                    pdf_previews = None
                    if is_pdf:
                        status.update(label="Конвертация PDF в изображения...", state="running")
                        pdf_previews = convert_pdf_to_images_and_store(s3, BUCKET_NAME, key, max_pages=3, zoom=2.0)
                        st.session_state["pdf_previews"] = pdf_previews
                        # Сохраняем число страниц PDF при наличии
                        if isinstance(pdf_previews, dict) and "page_count" in pdf_previews:
                            st.session_state["pdf_page_count"] = pdf_previews.get("page_count")

                    tex_resp = textract.detect_document_text(Document={"S3Object": {"Bucket": BUCKET_NAME, "Name": key}})
                    progress.progress(60)

                    # Подписи и печати
                    signature_hits = detect_signatures(textract, BUCKET_NAME, key, content_type)
                    # LLM определение печати (изображения: превью страниц PDF или само изображение для JPEG)
                    stamp_hits = {"stamp_present": None, "stamp_confidence": None, "qr_present": None, "qr_confidence": None, "raw": "", "error": None}
                    try:
                        bedrock = get_bedrock_client(AWS_PROFILE.strip() or None, BEDROCK_REGION)
                        imgs_content = []
                        if is_pdf and st.session_state.get("pdf_previews") and st.session_state["pdf_previews"].get("local_paths"):
                            # Используем локальные PNG превью
                            for lp in st.session_state["pdf_previews"]["local_paths"][:3]:
                                with open(lp, "rb") as f:
                                    imgs_content.append(_b64_image_from_bytes(f.read(), "image/png"))
                        else:
                            # Для JPEG: берём оригинальный объект из S3
                            if ("jpeg" in content_type.lower()) or ("jpg" in content_type.lower()) or key.lower().endswith((".jpg",".jpeg")):
                                obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                                bts = obj["Body"].read()
                                imgs_content.append(_b64_image_from_bytes(bts, "image/jpeg"))
                        if imgs_content:
                            stamp_llm = detect_stamp_llm(bedrock, MODEL_ID, imgs_content)
                            stamp_hits = stamp_llm
                    except Exception as e:
                        stamp_hits = {"stamp_present": None, "stamp_confidence": None, "qr_present": None, "qr_confidence": None, "raw": "", "error": str(e)}

                    status.update(label="Извлечение полей через Bedrock...", state="running")
                    extracted_text = textract_blocks_to_text(tex_resp)[:15000]
                    bedrock = get_bedrock_client(AWS_PROFILE.strip() or None, BEDROCK_REGION)
                    prompt = build_prompt_russian(extracted_text)
                    model_output = call_bedrock_invoke(MODEL_ID, prompt, bedrock)
                    progress.progress(90)

                    parsed = parse_json_relaxed(model_output)
                    if parsed is None:
                        parsed = {"Ошибка": "LLM вернул невалидный JSON"}

                    # Добавим сведения, введённые пользователем, в итоговый JSON
                    parsed["_client"] = {
                        "fio": st.session_state.get("client_fio"),
                        "doc_type": st.session_state.get("client_doc_type"),
                        # Добавляем короткое значение для дальнейшей сверки с ответами Bedrock
                        "doc_type_value": DOC_TYPE_VALUE_MAP.get(st.session_state.get("client_doc_type")),
                    }

                    parsed["_signatures"] = signature_hits
                    parsed["_stamps"] = stamp_hits

                    # --- Сохраняем результаты проверок в JSON (_checks) ---
                    try:
                        checks = {}
                        # ФИО
                        checks["fio_match"] = (norm_name(st.session_state.get("client_fio")) == norm_name(parsed.get("ФИО заявителя")))
                        # Тип документа
                        client_dt_norm = norm_doc_type(DOC_TYPE_VALUE_MAP.get(st.session_state.get("client_doc_type")))
                        bedrock_dt_norm = norm_doc_type(parsed.get("Тип документа"))
                        checks["doc_type_match"] = (client_dt_norm is not None and client_dt_norm == bedrock_dt_norm)
                        # Срок актуальности
                        doc_type_for_validity = bedrock_dt_norm or client_dt_norm
                        days = VALIDITY_DAYS.get(doc_type_for_validity) if doc_type_for_validity else None
                        issue_date = parse_date_safe(parsed.get("Дата выдачи документа"))
                        valid_until = None
                        is_valid_now = None
                        if days is not None and issue_date is not None:
                            valid_until = (issue_date.toordinal() + days)
                            valid_until_date = datetime.fromordinal(valid_until).date()
                            is_valid_now = datetime.utcnow().date() <= valid_until_date
                            checks["valid_until"] = valid_until_date.isoformat()
                            checks["is_valid_now"] = is_valid_now
                        else:
                            checks["valid_until"] = None
                            checks["is_valid_now"] = None
                        # Печать/QR
                        si = parsed.get("_stamps") if isinstance(parsed.get("_stamps"), dict) else {}
                        checks["stamp_or_qr_present"] = True if (si.get("stamp_present") is True or si.get("qr_present") is True) else (False if (si.get("stamp_present") is False and si.get("qr_present") is False) else None)
                        # PDF страницы
                        if st.session_state.get("last_is_pdf"):
                            pc = st.session_state.get("pdf_page_count")
                            checks["pdf_has_one_page"] = (pc == 1) if isinstance(pc, int) else None
                            checks["pdf_page_count"] = pc if isinstance(pc, int) else None
                        else:
                            checks["pdf_has_one_page"] = None
                            checks["pdf_page_count"] = None
                        parsed["_checks"] = checks

                        # --- Итоговый вердикт по проверкам ---
                        bools = [
                            checks.get("fio_match"),
                            checks.get("doc_type_match"),
                            checks.get("is_valid_now"),
                            checks.get("stamp_or_qr_present"),
                            checks.get("pdf_has_one_page"),
                        ]
                        evaluated_bools = [b for b in bools if isinstance(b, bool)]
                        if any(b is False for b in evaluated_bools):
                            checks["verdict"] = "fail"
                        elif evaluated_bools and all(b is True for b in evaluated_bools):
                            checks["verdict"] = "pass"
                        else:
                            checks["verdict"] = "unknown"

                        # --- Формируем список ошибок по стандарту МИБ ---
                        errors = []
                        def _push_err(field_key: str):
                            err = MIB_ERRORS.get(field_key)
                            if err:
                                errors.append({"field": field_key, "code": err.get("code"), "message": err.get("message")})

                        # ФИО не совпадает
                        if checks.get("fio_match") is False:
                            _push_err("ФИО заявителя и ФИО в документе должны совпадать")
                        # Тип документа не совпадает (используем код/сообщение для наименования документа)
                        if checks.get("doc_type_match") is False:
                            _push_err("Наименование документа")
                        # Срок актуальности истёк
                        if checks.get("is_valid_now") is False:
                            _push_err("Актуальная дата")
                        # Нет печати и нет QR
                        if checks.get("stamp_or_qr_present") is False:
                            _push_err("Наличие QR или печати")
                        # PDF содержит не одну страницу
                        if checks.get("pdf_has_one_page") is False:
                            _push_err("Прикрепленный файл должен содержать один документ")

                        parsed["_errors"] = errors
                    except Exception:
                        # Не ломаем процесс, если что-то пошло не так
                        parsed["_checks"] = {"error": "check_failed"}
                        parsed["_errors"] = [{"code": "unknown", "message": "check_failed"}]

                    status.update(label="Сохранение JSON в S3...", state="running")
                    folder = key.rsplit("/", 1)[0] + "/" if "/" in key else ""
                    json_key = f"{folder}extraction-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.json"
                    payload = json.dumps(parsed, ensure_ascii=False, indent=2).encode("utf-8")
                    bio = io.BytesIO(payload)
                    s3.upload_fileobj(
                        Fileobj=bio,
                        Bucket=BUCKET_NAME,
                        Key=json_key,
                        ExtraArgs={"ContentType": "application/json; charset=utf-8"},
                    )
                    status.update(label="Обработка завершена", state="complete")
                progress.progress(100)

                # ===================== РЕЗУЛЬТАТ (улучшенный UI) =====================
                st.markdown("### Результат")

                # Быстрые метрики и статусы
                signatures_info = parsed.get("_signatures") or {}
                stamps_info = parsed.get("_stamps") or {}
                signatures = signatures_info.get("signatures") or [] if isinstance(signatures_info, dict) else []
                # Для обратной совместимости не используем это значение напрямую
                stamp_present = stamps_info.get("stamp_present") if isinstance(stamps_info, dict) else None

                # Сообщение об ошибках извлечения
                llm_error = parsed.get("Ошибка")
                sig_err = signatures_info.get("error") if isinstance(signatures_info, dict) else None
                stamp_err = stamps_info.get("error") if isinstance(stamps_info, dict) else None
                if llm_error:
                    st.error(f"Ошибка парсинга LLM: {llm_error}")
                if sig_err:
                    st.warning(f"Ошибка при обнаружении подписей: {sig_err}")
                if stamp_err:
                    st.warning(f"Ошибка при обнаружении печатей: {stamp_err}")

                # Табы: Проверка | Детальная проверка | Превью | Структура | JSON
                tab_verify, tab_detail, tab_preview, tab_structure, tab_json = st.tabs(["Сводная проверка", "Детальная проверка", "Превью документа", "Структурированные поля", "Сырые данные (JSON)"])

                # --- Структурированные поля ---
                with tab_structure:
                    # Отфильтровать служебные ключи
                    user_fields = {k: v for k, v in parsed.items() if not str(k).startswith("_") and k != "Ошибка"}
                    if not user_fields:
                        st.info("Нет извлечённых полей для отображения.")
                    else:
                        # Табличное представление: одна строка = одна пара (ключ, значение)
                        items = list(user_fields.items())
                        rows = [{"Поле": k, "Значение": (v if v not in (None, "") else "—")} for k, v in items]

                        # Добавляем агрегат по подписям как отдельную запись
                        try:
                            if signatures:
                                confidences = [s.get("confidence") for s in signatures if isinstance(s, dict) and s.get("confidence") is not None]
                                if confidences:
                                    max_conf = max(confidences)
                                    # Textract возвращает [0..100]
                                    cr_text = f"обнаружен (CR {round(max_conf)}%)"
                                else:
                                    cr_text = "обнаружен"
                            else:
                                cr_text = "не обнаружен"
                        except Exception:
                            cr_text = "не обнаружен"
                        # Не добавляем сразу; перенесём в конец таблицы

                        # Добавляем агрегат по печати из LLM
                        try:
                            if isinstance(stamps_info, dict) and stamps_info.get("stamp_present") is True:
                                conf = stamps_info.get("stamp_confidence")
                                if isinstance(conf, (int, float)):
                                    stamp_text = f"обнаружена (CR {round(conf)}%)"
                                else:
                                    stamp_text = "обнаружена"
                            elif isinstance(stamps_info, dict) and stamps_info.get("stamp_present") is False:
                                stamp_text = "не обнаружена"
                            else:
                                stamp_text = "не определено"
                        except Exception:
                            stamp_text = "не определено"
                        
                        # Добавляем агрегат по QR из LLM
                        try:
                            if isinstance(stamps_info, dict) and stamps_info.get("qr_present") is True:
                                qconf = stamps_info.get("qr_confidence")
                                if isinstance(qconf, (int, float)):
                                    qr_text = f"обнаружен (CR {round(qconf)}%)"
                                else:
                                    qr_text = "обнаружен"
                            elif isinstance(stamps_info, dict) and stamps_info.get("qr_present") is False:
                                qr_text = "не обнаружен"
                            else:
                                qr_text = "не определено"
                        except Exception:
                            qr_text = "не определено"

                        # Перемещаем "Подпись", "Печать" и "QR-код" в конец списка
                        rows.extend([
                            {"Поле": "Подпись", "Значение": cr_text},
                            {"Поле": "Печать", "Значение": stamp_text},
                            {"Поле": "QR-код", "Значение": qr_text},
                        ])

                        # Используем индекс DataFrame, начиная с 1 (без отдельной колонки "№")
                        df = pd.DataFrame(rows)
                        df.index = range(1, len(df) + 1)
                        st.table(df)

                # --- Превью документа ---
                with tab_preview:
                    st.markdown("#### Превью документа")
                    previews = st.session_state.get("pdf_previews") if "pdf_previews" in st.session_state else None
                    if previews and not previews.get("error") and previews.get("local_paths"):
                        for p in previews["local_paths"][:3]:
                            st.image(p, caption=os.path.basename(p), use_container_width=True)
                        if previews.get("s3_keys"):
                            st.caption("S3 превью:")
                            for k in previews["s3_keys"]:
                                st.code(f"s3://{BUCKET_NAME}/{k}")
                    elif previews and previews.get("error"):
                        st.warning(f"Не удалось сгенерировать превью документа: {previews['error']}")
                    else:
                        st.caption("Превью доступно только для PDF-файлов после загрузки.")

                # --- Проверка соответствий (первый таб) ---
                with tab_verify:
                    errors_list = parsed.get("_errors") or []
                    checks = parsed.get("_checks") or {}
                    # Подсчёт проверок, по которым есть решение (True/False)
                    evaluated = [v for v in [
                        checks.get("fio_match"),
                        checks.get("doc_type_match"),
                        checks.get("is_valid_now"),
                        checks.get("stamp_or_qr_present"),
                        checks.get("pdf_has_one_page"),
                    ] if isinstance(v, bool)]
                    total_evaluated = len(evaluated)
                    fails = sum(1 for v in evaluated if v is False)
                    passes = sum(1 for v in evaluated if v is True)

                    csum1, csum2, csum3 = st.columns(3)
                    with csum1:
                        st.metric(label="Проверки выполнены", value=total_evaluated)
                    with csum2:
                        st.metric(label="Успешно", value=passes)
                    with csum3:
                        st.metric(label="Ошибки", value=len(errors_list))

                    # Итоговый вердикт
                    verdict = checks.get("verdict")
                    if verdict == "pass":
                        st.success("Итог: документ прошёл проверку.")
                    elif verdict == "fail":
                        st.error("Итог: документ не прошёл проверку.")
                    else:
                        st.info("Итог: недостаточно данных для окончательного вердикта.")

                # --- Детальная проверка (вся подробная информация) ---
                with tab_detail:
                    render_detailed_checks(parsed)

                # --- Сырые данные ---
                with tab_json:
                    st.json(parsed)
                    st.download_button(
                        label="Скачать JSON",
                        data=json.dumps(parsed, ensure_ascii=False, indent=2).encode("utf-8"),
                        file_name="extraction.json",
                        mime="application/json",
                        use_container_width=True,
                    )

            except ClientError as e:
                err = e.response.get("Error", {})
                st.error(f"Ошибка обработки: {err.get('Code', 'Unknown')} - {err.get('Message', str(e))}")
            except Exception as e:
                st.error(f"Обработка не удалась: {e}")

        except NoCredentialsError:
            st.error("AWS-учётные данные не найдены. Настройте их через ~/.aws/credentials или переменные окружения.")
        except ClientError as e:
            err = e.response.get("Error", {})
            st.error(f"AWS ClientError: {err.get('Code', 'Unknown')} - {err.get('Message', str(e))}")
        except (BotoCoreError, Exception) as e:
            st.error(f"Ошибка при загрузке: {e}")
