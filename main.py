import os
from datetime import datetime
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
from botocore.exceptions import BotoCoreError, NoCredentialsError, ClientError
import streamlit as st

# ======================= UI ЧАСТЬ =========================
st.set_page_config(page_title="S3 File Uploader", layout="centered")

st.write("")
st.title("RB Loan Deferment IDP sds")
st.write("Загрузите один файл в Amazon S3 для последующей обработки.")

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

with st.expander("Помощь и настройка", expanded=False):
    tabs = st.tabs(["Запуск приложения", "Создание Access Key", "Окружение"])
    with tabs[0]:
        st.markdown("#### 1) Установите AWS CLI v2")
        st.code('''curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"\nsudo installer -pkg AWSCLIV2.pkg -target /''', language="bash")
        st.code("aws --version", language="bash")
        st.markdown("#### 2) Настройте учётные данные")
        st.code("aws configure", language="bash")
        st.markdown("#### 3) Запустите приложение")
        st.code("streamlit run main.py", language="bash")
    with tabs[1]:
        st.markdown("### 🔑 Создание Access Key (CLI)")
        st.markdown("Программные ключи нужны для работы из кода/CLI. Создайте их в AWS IAM.")
    with tabs[2]:
        st.markdown("### Окружение")
        st.markdown(f"- Bucket: `{BUCKET_NAME}`\n- Region: `{AWS_REGION}`\n- Model: `{MODEL_ID}`")
        # Настройка Inference Profile через UI / ENV
        default_ip = (
            os.getenv("BEDROCK_INFERENCE_PROFILE")
            or DEFAULT_INFERENCE_PROFILE_ARN
            or DEFAULT_INFERENCE_PROFILE_ID
        )
        ip_value = st.text_input(
            "Inference Profile (ID или ARN для Claude 3.7 Sonnet)",
            value=st.session_state.get("inference_profile", default_ip),
            help="Например ID: us.anthropic.claude-3-7-sonnet-20250219-v1:0 или ARN: arn:aws:bedrock:...:inference-profile/us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        )
        st.session_state["inference_profile"] = ip_value.strip() if ip_value else ""

# --- Форма загрузки ---
with st.form("upload_form", clear_on_submit=False):
    uploaded_file = st.file_uploader(
        "Выберите документ",
        type=["pdf", "jpg"],
        accept_multiple_files=False,
        help="Поддержка: PDF, JPEG",
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

            # Backoff функция
            def get_document_analysis_with_backoff(job_id, max_retries=6):
                retries = 0
                while True:
                    try:
                        resp = textract_client.get_document_analysis(JobId=job_id, MaxResults=1000)
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
                resp = get_document_analysis_with_backoff(job_id)
                status = resp["JobStatus"]
                if status == "SUCCEEDED":
                    pages.append(resp)
                    next_token = resp.get("NextToken")
                    while next_token:
                        nxt = get_document_analysis_with_backoff(job_id)
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
            "Определи, есть ли на изображении отсканированного документа печать (штамп/круглая/прямоугольная).")
        format_req = (
            "Верни строго JSON без пояснений:\n"
            "{\n  \"stamp_present\": true|false,\n  \"confidence\": number (0..100),\n  \"reason\": string\n}"
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
            return {"present": None, "confidence": None, "reason": None, "raw": text, "error": "LLM returned non-JSON"}
        return {
            "present": parsed.get("stamp_present"),
            "confidence": parsed.get("confidence"),
            "reason": parsed.get("reason"),
            "raw": text,
            "error": None,
        }
    except Exception as e:
        return {"present": None, "confidence": None, "reason": None, "raw": "", "error": str(e)}

def convert_pdf_to_images_and_store(s3_client, bucket: str, key: str, max_pages: int = 3, zoom: float = 2.0):
    """
    Конвертация первых max_pages страниц PDF (из S3) в PNG изображения.
    Сохраняет локально в /tmp и загружает в S3 по пути previews/page_XXX.png.

    Возвращает dict: {"local_paths": [..], "s3_keys": [..], "error": None|str}
    """
    if fitz is None:
        return {"local_paths": [], "s3_keys": [], "error": "PyMuPDF (fitz) не установлен"}
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        pdf_bytes = obj["Body"].read()

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = min(len(doc), max_pages)
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

        return {"local_paths": local_paths, "s3_keys": s3_keys, "error": None}
    except Exception as e:
        return {"local_paths": [], "s3_keys": [], "error": str(e)}

def build_prompt_russian(extracted_text: str) -> str:
    instruction = (
        "Извлеки следующую информацию из текста.\n"
        "Верни результат строго в формате JSON:\n"
        "{\n"
        "  \"Название документа\": string | null,\n"
        "  \"Номер документа\": string | null,\n"
        "  \"Название компании\": string | null,\n"
        "  \"Дата выдачи справки\": string | null,\n"
        "  \"Дата начала отпуска\": string | null,\n"
        "  \"Дата окончания отпуска\": string | null,\n"
        "  \"ФИО кому предоставляется отпуск\": string | null,\n"
        "  \"город\": string | null\n"
        "}\n\n"
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
    elif not uploaded_file:
        st.error("Не выбран файл для загрузки.")
    else:
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
                status.update(label="Файл загружен", state="complete")
            progress.progress(30)

            s3_uri = f"s3://{BUCKET_NAME}/{key}"
            st.success(f"Файл успешно загружен в {s3_uri}")

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
                    pdf_previews = None
                    if is_pdf:
                        status.update(label="Конвертация PDF в изображения...", state="running")
                        pdf_previews = convert_pdf_to_images_and_store(s3, BUCKET_NAME, key, max_pages=3, zoom=2.0)
                        st.session_state["pdf_previews"] = pdf_previews

                    tex_resp = textract.detect_document_text(Document={"S3Object": {"Bucket": BUCKET_NAME, "Name": key}})
                    progress.progress(60)

                    # Подписи и печати
                    signature_hits = detect_signatures(textract, BUCKET_NAME, key, content_type)
                    # LLM определение печати (изображения: превью страниц PDF или само изображение для JPEG)
                    stamp_hits = {"present": None, "confidence": None, "reason": None, "raw": "", "error": None}
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
                        stamp_hits = {"present": None, "confidence": None, "reason": None, "raw": "", "error": str(e)}

                    status.update(label="Извлечение полей через Bedrock...", state="running")
                    extracted_text = textract_blocks_to_text(tex_resp)[:15000]
                    bedrock = get_bedrock_client(AWS_PROFILE.strip() or None, BEDROCK_REGION)
                    prompt = build_prompt_russian(extracted_text)
                    model_output = call_bedrock_invoke(MODEL_ID, prompt, bedrock)
                    progress.progress(90)

                    try:
                        parsed = json.loads(model_output)
                    except Exception:
                        start = model_output.find("{")
                        end = model_output.rfind("}")
                        parsed = None
                        if start != -1 and end != -1 and end > start:
                            try:
                                parsed = json.loads(model_output[start:end+1])
                            except Exception:
                                parsed = None
                    if parsed is None:
                        parsed = {"Ошибка": "LLM вернул невалидный JSON"}

                    parsed["_signatures"] = signature_hits
                    parsed["_stamps"] = stamp_hits

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
                stamp_present = stamps_info.get("present") if isinstance(stamps_info, dict) else None

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

                # Табы: Структура | Диагностика | JSON
                tab_structure, tab_diag, tab_json = st.tabs(["Структурированные поля", "Диагностика", "Сырые данные (JSON)"])

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
                            if isinstance(stamps_info, dict) and stamps_info.get("present") is True:
                                conf = stamps_info.get("confidence")
                                if isinstance(conf, (int, float)):
                                    stamp_text = f"обнаружена (CR {round(conf)}%)"
                                else:
                                    stamp_text = "обнаружена"
                            elif isinstance(stamps_info, dict) and stamps_info.get("present") is False:
                                stamp_text = "не обнаружена"
                            else:
                                stamp_text = "не определено"
                        except Exception:
                            stamp_text = "не определено"
                        # Перемещаем "Подпись" и "Печать" в конец списка
                        rows.extend([
                            {"Поле": "Подпись", "Значение": cr_text},
                            {"Поле": "Печать", "Значение": stamp_text},
                        ])

                        # Используем индекс DataFrame, начиная с 1 (без отдельной колонки "№")
                        df = pd.DataFrame(rows)
                        df.index = range(1, len(df) + 1)
                        st.table(df)

                # --- Диагностика ---
                with tab_diag:
                    st.markdown("#### Обнаружение подписей")
                    if signatures:
                        st.json({"count": len(signatures), "items": signatures})
                    else:
                        st.caption("Подписи не обнаружены.")

                    st.markdown("#### Обнаружение печати (LLM)")
                    if isinstance(stamps_info, dict):
                        st.json(stamps_info)
                    else:
                        st.caption("Результат определения печати отсутствует.")

                    # Превью PDF страниц (если были сгенерированы)
                    previews = st.session_state.get("pdf_previews") if "pdf_previews" in st.session_state else None
                    if previews and not previews.get("error") and previews.get("local_paths"):
                        st.markdown("#### Превью страниц (PDF → PNG)")
                        # Показываем до 3 изображений
                        for p in previews["local_paths"][:3]:
                            st.image(p, caption=os.path.basename(p), use_container_width=True)
                        if previews.get("s3_keys"):
                            st.caption("S3 превью:")
                            for k in previews["s3_keys"]:
                                st.code(f"s3://{BUCKET_NAME}/{k}")
                    elif previews and previews.get("error"):
                        st.warning(f"Не удалось сгенерировать превью PDF: {previews['error']}")

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
