import requests

# Author: Joseluis Garcia
# jgarcia23

# Delete method
def delete(session, get_url, command_url, type):
    get_data = get_url

    command_endpoint = command_url
    ids = []
    payload = []

    next_pg = True
    while next_pg: # iterate until next page == null
        r = session.get(get_data)
        result = r.json()
        for file in result[type]: # for each
            id = file['id']
            ids.append(id)

        url_paste = ",".join(str(x) for x in ids) # join each var in ids change type to string and join with ','
        url = command_endpoint + url_paste
        d = session.delete(url)
        delete_result = d.json()
        print(delete_result)
        nxt = result['next_page']
        ids = []
        if nxt is None:
            next_pg = False

        get_data = result['next_page']


def main():
    # creates a requests session object and configures it with your authentication information.
    session = requests.Session()
    session.headers = {'Content-Type': 'application/json'}
    session.auth = 'username', 'password'
    # URL End-points
    ticket_url = 'https://z3nplatformdevjg.zendesk.com/api/v2/tickets.json'
    delete_tickets_url = 'https://z3nplatformdevjg.zendesk.com/api/v2/tickets/destroy_many.json?ids='
    users_url = 'https://z3nplatformdevjg.zendesk.com/api/v2/users.json'
    delete_users_url = 'https://z3nplatformdevjg.zendesk.com/api/v2/users/destroy_many.json?ids='

    delete(session, ticket_url, delete_tickets_url, 'tickets') # delete tickets
    delete(session,users_url, delete_users_url,'users')# delete users


if __name__ == '__main__':
    main()
