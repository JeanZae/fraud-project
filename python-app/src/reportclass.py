import base64
from io import BytesIO
import pandas as pd

class SimpleReport:
    def __init__(self, file_path="/outputs/report.html"):
        self.file_path = file_path
        self.content = []
        self.image_counter = 0
        
    def add_text(self, text):
        """Добавляет текст в отчет"""
        self.content.append(f"<p>{text}</p>")
        
    def add_dataframe(self, df, title="", max_rows=10):
        """Добавляет таблицу в отчет"""
        if title:
            self.content.append(f"<h3>{title}</h3>")
        self.content.append(df.head(max_rows).to_html())
        
    def add_plot(self, plt, title=""):
        """Сохраняет график как картинку в HTML"""
        self.image_counter += 1
        
        # Сохраняем график в base64 (встроенная картинка)
        buffer = BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.getvalue()).decode()
        
        if title:
            self.content.append(f"<h3>{title}</h3>")
        self.content.append(f'<img src="data:image/png;base64,{image_base64}" width="600">')
        plt.close()

    def save(self):
        """Сохраняет весь отчет в файл"""
        html_content = f"""
        <html>
        <head><title>Отчет</title></head>
        <body>
        {' '.join(self.content)}
        </body>
        </html>
        """
        
        with open(self.file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Отчет сохранен: {self.file_path}")

# Глобальный объект отчета
report = SimpleReport()