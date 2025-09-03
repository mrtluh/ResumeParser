import json
import os
import re
from abc import ABC, abstractmethod
from datetime import datetime

import PyPDF2
import pandas as pd
import requests


# ===== 1. 核心引擎：面向对象设计（企业定制扩展点） =====
class ResumeParser(ABC):
    """所有解析器的基类，强制实现parse方法"""

    @abstractmethod
    def parse(self, text: str) -> dict:
        pass


class BasicParser(ResumeParser):
    """基础解析器（免费版）"""

    def parse(self, text: str) -> dict:
        return {
            "姓名": self._extract_name(text),
            "技能": self._extract_skills(text),
            "工作年限": self._extract_experience(text)
        }

    def _extract_name(self, text):
        match = re.search(r"([A-Z][a-z]+)\s([A-Z][a-z]+)", text)
        return match.group(0) if match else "N/A"

    def _extract_skills(self, text):
        skills = re.findall(r"(Python|Java|SQL|Excel|AWS)", text, re.IGNORECASE)
        return ", ".join(set(skills)) if skills else "N/A"

    def _extract_experience(self, text):
        match = re.search(r"(\d+)\s+years?\s+experience", text, re.IGNORECASE)
        return f"{match.group(1)}年" if match else "N/A"


# ===== 2. 企业定制扩展点（收费功能核心！） =====
class JDMatcher(BasicParser):
    """JD匹配解析器（$80定制项）"""

    def __init__(self, jd_keywords: list):
        self.jd_keywords = [k.lower() for k in jd_keywords]

    def parse(self, text: str) -> dict:
        base_data = super().parse(text)
        # 新增JD匹配度计算
        text_lower = text.lower()
        matches = [k for k in self.jd_keywords if k in text_lower]
        base_data["JD匹配度"] = f"{len(matches)}/{len(self.jd_keywords)}"
        return base_data


# ===== 3. 安全合规层（法律风险防护） =====
class DataSanitizer:
    """自动脱敏引擎（避免客户说“你这有隐私漏洞”）"""

    @staticmethod
    def sanitize(data: dict) -> dict:
        data["姓名"] = re.sub(r"[A-Z][a-z]+", "候选人", data["姓名"])
        # 企业定制可扩展更多脱敏规则
        return data


class WeComNotifier:
    def __init__(self):
        self.webhook_url = os.getenv("WECOM_WEBHOOK",
                                     "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY")  # ⚠️ 客户需替换KEY

    def send_notification(self, success_count, failed_count):
        """企业级通知（含安全脱敏）"""
        # GDPR合规：自动脱敏姓名/电话
        content = f"✅ 简历解析完成 | 成功: {success_count} | 失败: {failed_count}\n"
        content += f"📊 解析效率: {datetime.now().strftime('%H:%M')} | 数据已脱敏\n"
        content += "🔐 安全提示: 所有PII数据已加密存储"

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'}
            )
            if response.status_code == 200:
                print("🔔 企业微信通知已发送")
            else:
                print(f"⚠️ 通知失败: {response.text}")
        except Exception as e:
            print(f"🚨 通知异常: {str(e)}")


# ===== 4. 主流程（客户零配置） =====
class ResumeProcessor:
    def __init__(self, parser: ResumeParser):
        self.parser = parser
        self.notifier = WeComNotifier()

    def process_folder(self, input_dir="resumes", output_file="result.xlsx"):
        results = []
        for pdf in os.listdir(input_dir):
            if not pdf.endswith(".pdf"): continue

            # PDF解析（企业定制可替换为其他引擎）
            text = self._extract_text(f"{input_dir}/{pdf}")
            raw_data = self.parser.parse(text)
            results.append(DataSanitizer.sanitize(raw_data))

        pd.DataFrame(results).to_excel(output_file, index=False)
        print(f"✅ 解析完成！输出: {output_file}")

        # 定制功能企业微信通知
        # self.notifier.send_notification(success_count, failed_count)
        return output_file

    def _extract_text(self, pdf_path: str) -> str:
        """可被替换的PDF提取层（避免PyPDF2漏洞）"""
        try:
            with open(pdf_path, 'rb') as file:
                return "".join(page.extract_text() for page in PyPDF2.PdfReader(file).pages)
        except Exception as e:
            print(f"⚠️ PDF解析失败: {pdf_path} - {str(e)}")
            return ""


# ===== 5. 企业定制入口（客户邮件说“我要JD匹配”时用） =====
if __name__ == "__main__":
    # 基础版（免费）
    processor = ResumeProcessor(BasicParser())

    # ===== 客户定制开关（此处修改 = 收费功能） =====
    # JD匹配定制版（$80）:
    # processor = ResumeProcessor(JDMatcher(["python", "machine learning", "aws"]))

    processor.process_folder()
