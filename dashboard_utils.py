import panel as pn

# function to update the loading message
def update_loading_message(template, message):
    template.main[0][0][2].object = message
    
# function to display database error
def display_database_error(template: pn.template.MaterialTemplate, show_alert:bool = False):

    if not show_alert:
        template.main[0][0] = pn.Column()
        template.sidebar[0][0] = pn.Column()
        db_error_image = pn.pane.PNG('./images/db_error.png', width=100, align='center')
        db_error_text = pn.pane.Markdown('''<h1 style="text-align:center">Connection to database failed. Check if the database is running.</h1>''')
        main_error_col = pn.Column(pn.layout.VSpacer(), db_error_image,  db_error_text, pn.layout.VSpacer(), sizing_mode='stretch_both')
        template.main[0][0] = main_error_col
    else:
        print('showing alert')
        alert = pn.pane.Alert('## Alert\nThis is a warning!')        
        template.main[0].insert(0, alert)
