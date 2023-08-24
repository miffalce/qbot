import sys
import os
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/plugins"))
sys.path.append(
    os.path.join(os.path.dirname(__file__), "../src/plugins/nonebot_plugin_spam/")
)


load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"), interpolate=False)


# @pytest.fixture(autouse=True)
# def control_imports(request):
#     if "skip_init_plugin" in request.keywords:
#         sys.modules["nonebot_plugin_spam"] = MagicMock()
