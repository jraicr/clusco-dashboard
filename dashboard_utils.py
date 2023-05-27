import panel as pn

"""
Module with utility functions for the dashboard.
"""

def update_loading_message(template:pn.template.MaterialTemplate, message:str):
    """
    Updates and shows a loading message in the dashboard while deploying it for the first time.

    Parameters
    ----------
    - `template` (pn.template.MaterialTemplate) The template object from panel.
    - `message` (str) The message to display in the loading screen.
    """
    template.main[0][0][2].object = message
    
def display_database_error(template: pn.template.MaterialTemplate):
    """
    Display a message in the dashboard when the connection to the database fails.

    Parameters
    ----------
    - `template` (pn.template.MaterialTemplate) The template object from panel.
    """
    template.main[0][0] = pn.Column()
    template.sidebar[0][0] = pn.Column()
    db_error_image = pn.pane.PNG('./images/db_error.png', width=100, align='center')
    db_error_text = pn.pane.Markdown('''<h1 style="text-align:center">Connection to database failed. Check if database is running or contact with a system administrator.</h1>''')
    main_error_col = pn.Column(pn.layout.VSpacer(), db_error_image,  db_error_text, pn.layout.VSpacer(), sizing_mode='stretch_both')
    template.main[0][0] = main_error_col
