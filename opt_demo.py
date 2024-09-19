from flask import Flask, request

def flasky(test_data: dict, host = 'localhost', port = 8085):
    app = Flask('Flasky')

    @app.route('/', methods = ['GET', 'POST'])
    def home():
        if request.method == 'GET':
            return 'OK'
        elif request.method == 'POST':
            if result := (test_data.get(request.json.get('test'))) is not None:
                return result
            return 'Bad Info'
    
    app.run(host = host, port = port, debug = True)
    



# if __name__ == '__main__':
#     flasky({'temp': 'yes'})
#     print(123)