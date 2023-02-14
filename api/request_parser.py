import proto_message_pb2 as bqm

def parse_message(req, proto_class):
    proto = proto_class()
    for attributes in req.keys():
        try:
            setattr(proto, attributes, req[attributes])
        except AttributeError as e:
            raise e 
        except TypeError as e:
            raise e 
    return proto

def parse_request(request, proto_class):
    json_request = request.get_json()
    try:
        current_record = 1
        parsed_request = []
        for row in json_request:
            parsed_request.append(parse_message(row, proto_class))
            current_record = current_record + 1
        return parsed_request
    except AttributeError as e:
        error_string = str(e)
        return ({"msg": f"{error_string} on record number {current_record}"}), 400  
    except TypeError as e:
        error_string = str(e)
        return ({"msg": f"{error_string} on record number {current_record}"}), 400  
        
class Dummy_request():
    def __init__(self, json_request):
        self.json_request = json_request
    
    def get_json(self):
        return self.json_request

if __name__ == '__main__':
    # this are tests for the previous functions
    good_job_request = Dummy_request([
        {'id': 184, 'job': 'Test Job'},
        {'id': 185, 'job': 'Test Job'},
        {'id': 186, 'job': 'Test Job'},
        {'id': 187, 'job': 'Test Job'},
        {'id': 188, 'job': 'Test Job'}
    ])
    print(parse_request(good_job_request, bqm.Job))

    # or a bad request
    bad_job_request_attr_1 = Dummy_request([
        {'id': 184, 'job': 'Test Job'},
        {'id': 185, 'job': 'Test Job'},
        {'id': 186, 'job': 'Test Job'},
        {'idi': 187, 'job': 'Test Job'},
        {'id': 188, 'job': 'Test Job'}
    ])
    print(parse_request(bad_job_request_attr_1, bqm.Job))

    bad_job_request_attr_2 = Dummy_request([
        {'id': 184, 'job': 'Test Job'},
        {'id': 185, 'job': 'Test Job'},
        {'id': '186', 'job': 'Test Job'},
        {'id': 187, 'job': 'Test Job'},
        {'id': 188, 'job': 'Test Job'}
    ])

    print(parse_request(bad_job_request_attr_2, bqm.Job))