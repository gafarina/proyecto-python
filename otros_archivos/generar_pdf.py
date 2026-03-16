import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="guia_entorno_virtual.pdf"):
    # Configurar el documento con márgenes más amplios
    doc = SimpleDocTemplate(
        filename,
        pagesize=A4,
        rightMargin=1 * inch,
        leftMargin=1 * inch,
        topMargin=1 * inch,
        bottomMargin=1 * inch
    )

    # Estilos
    styles = getSampleStyleSheet()
    
    # Estilo del Título Principal
    title_style = ParagraphStyle(
        name='MainTitle',
        parent=styles['Title'],
        fontName='Helvetica-Bold',
        fontSize=24,
        leading=28,
        textColor=colors.HexColor("#2A4365"), # Azul oscuro profesional
        spaceAfter=20,
        alignment=1 # Centrado
    )

    # Estilo de Introducción
    intro_style = ParagraphStyle(
        name='Intro',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=18,
        textColor=colors.HexColor("#4A5568"), # Gris texto
        spaceAfter=20
    )

    # Estilo de Título de Paso
    step_title_style = ParagraphStyle(
        name='StepTitle',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#2B6CB0"), # Azul vibrante
        spaceBefore=15,
        spaceAfter=8
    )

    # Estilo de Texto Normal
    body_style = ParagraphStyle(
        name='Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=11,
        leading=16,
        textColor=colors.HexColor("#2D3748"),
        spaceAfter=8
    )

    # Estilo de Bloque de Código (Terminal)
    code_style = ParagraphStyle(
        name='CodeStyle',
        parent=styles['Normal'],
        fontName='Courier',
        fontSize=10,
        leading=14,
        textColor=colors.HexColor("#E2E8F0"), # Texto claro
        backColor=colors.HexColor("#1A202C"), # Fondo oscuro
        borderPadding=(8, 10, 8, 10),
        borderWidth=1,
        borderColor=colors.HexColor("#2D3748"),
        borderRadius=5,
        spaceBefore=5,
        spaceAfter=15
    )
    
    # Inline code (para comandos cortos dentro de párrafos)
    # Se usa usando tags <font name="Courier" color="#D53F8C">comando</font>

    story = []

    # Encabezado
    story.append(Paragraph("Guía Profesional de Python", ParagraphStyle(
        name='Sup', fontName='Helvetica-Bold', fontSize=10, textColor=colors.HexColor("#A0AEC0"), alignment=1, spaceAfter=5
    )))
    story.append(Paragraph("Creación de Entornos Virtuales", title_style))
    story.append(Spacer(1, 0.2 * inch))

    # Introducción
    intro_text = (
        "Un entorno virtual (<b>venv</b>) es una herramienta indispensable en el desarrollo profesional con Python. "
        "Permite mantener las dependencias requeridas por diferentes proyectos completamente aisladas, "
        "evitando conflictos entre versiones de librerías."
    )
    story.append(Paragraph(intro_text, intro_style))

    # Definimos los pasos
    steps_data = [
        {
            "title": "1. Verificación de Python",
            "body": "Antes de comenzar, asegúrate de tener Python instalado correctamente en tu sistema. Si lo instalas desde cero, no olvides marcar la opción <b>'Add Python to PATH'</b> en el instalador.",
            "code": "python --version"
        },
        {
            "title": "2. Navegar al Directorio del Proyecto",
            "body": "Abre tu terminal favorita (PowerShell, CMD, o la terminal de VS Code) y navega hasta la carpeta raíz donde residirá tu proyecto.",
            "code": "cd C:\\ruta\\a\\tu\\proyecto"
        },
        {
            "title": "3. Crear el Entorno Virtual",
            "body": "Ejecuta el módulo venv de Python para generar la estructura de carpetas del entorno. Por convención, a esta carpeta se le suele llamar <font name='Courier' color='#D53F8C'>venv</font> o <font name='Courier' color='#D53F8C'>.venv</font>.",
            "code": "python -m venv venv"
        },
        {
            "title": "4. Activar el Entorno",
            "body": "Para empezar a usar el entorno aislado, debes activarlo. El comando varía ligeramente según tu sistema operativo y shell:",
            "code": "Windows PowerShell:\n.\\venv\\Scripts\\Activate.ps1\n\nWindows CMD:\n.\\venv\\Scripts\\activate.bat\n\nLinux / macOS:\nsource venv/bin/activate"
        },
        {
            "title": "5. Gestión de Dependencias",
            "body": "Una vez activado (verás <font name='Courier' color='#D53F8C'>(venv)</font> en tu terminal), todas las instalaciones usando pip se guardarán exclusivamente en este entorno.",
            "code": "pip install nombre-del-paquete\n\n# Para guardar tus dependencias:\npip freeze > requirements.txt"
        },
        {
            "title": "6. Desactivar el Entorno",
            "body": "Cuando finalices tu trabajo o necesites cambiar a otro proyecto, simplemente desactiva el entorno para volver a la configuración global del sistema.",
            "code": "deactivate"
        }
    ]

    # Agregar los pasos al documento
    for step in steps_data:
        # Usamos KeepTogether para intentar no cortar un paso a la mitad de dos páginas
        step_flowables = [
            Paragraph(step["title"], step_title_style),
            Paragraph(step["body"], body_style)
        ]
        
        # Procesar los saltos de línea en el bloque de código
        code_text = step["code"].replace('\n', '<br/>')
        step_flowables.append(Paragraph(code_text, code_style))
        
        story.append(KeepTogether(step_flowables))
        story.append(Spacer(1, 10))

    # Pie de página (se puede agregar con PageTemplates pero para algo sencillo lo ponemos al final)
    story.append(Spacer(1, 30))
    footer_style = ParagraphStyle(
        name='Footer',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        textColor=colors.HexColor("#A0AEC0"),
        alignment=1
    )
    story.append(Paragraph("Generado de forma automatizada. Buenas prácticas de desarrollo en Python.", footer_style))

    # Construir el PDF
    doc.build(story)
    print(f"PDF generado exitosamente en: {os.path.abspath(filename)}")

if __name__ == "__main__":
    create_pdf()
