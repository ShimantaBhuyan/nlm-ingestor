import logging
import nlm_ingestor.ingestion_daemon.config as cfg
import os
import tempfile
import traceback
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
from nlm_ingestor.ingestor import ingestor_api
from nlm_utils.utils import file_utils
from hypercorn.config import Config
from hypercorn.asyncio import serve
import asyncio

app = Flask(__name__)

# initialize logging
logger = logging.getLogger(__name__)
logger.setLevel(cfg.log_level())

@app.route('/', methods=['GET'])
def health_check():
    return 'Service is running', 200

@app.route('/api/parseDocument', methods=['POST'])
def parse_document():
    render_format = request.args.get('renderFormat', 'all')
    use_new_indent_parser = request.args.get('useNewIndentParser', 'no')
    apply_ocr = request.args.get('applyOcr', 'no')
    file = request.files['file']
    tmp_file = None
    try:
        parse_options = {
            "parse_and_render_only": True,
            "render_format": render_format,
            "use_new_indent_parser": use_new_indent_parser == "yes",
            "parse_pages": (),
            "apply_ocr": apply_ocr == "yes"
        }
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as temp:
            file.save(temp.name)
            tmp_file = temp.name
        
        props = file_utils.extract_file_properties(tmp_file)
        logger.info(f"Parsing document: {secure_filename(file.filename)}")
        return_dict, _ = ingestor_api.ingest_document(
            secure_filename(file.filename),
            tmp_file,
            props["mimeType"],
            parse_options=parse_options,
        )
        return jsonify({"status": 200, "return_dict": return_dict or {}})

    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}", exc_info=True)
        return jsonify({"status": "fail", "reason": str(e)}), 500

    finally:
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)

async def main():
    logger.info("Starting ingestor service..")
    hypercorn_config = Config()
    hypercorn_config.bind = ["[::]:5001"] if os.environ.get("ENVIRONMENT") != "development" else ["0.0.0.0:5001"]
    # hypercorn_config.workers = 4  # Adjust the number of workers as needed
    await serve(app, hypercorn_config)

if __name__ == "__main__":
    asyncio.run(main())
