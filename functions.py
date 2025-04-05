"""
Course: CSE 251, week 14
File: functions.py
Author: <Grant Jones>

Instructions:

Depth First Search
https://www.youtube.com/watch?v=9RHO6jU--GU

Breadth First Search
https://www.youtube.com/watch?v=86g8jAQug04


Requesting a family from the server:
family_id = 6128784944
request = Request_thread(f'{TOP_API_URL}/family/{family_id}')
request.start()
request.join()

Example JSON returned from the server
{
    'id': 6128784944, 
    'husband_id': 2367673859,        # use with the Person API
    'wife_id': 2373686152,           # use with the Person API
    'children': [2380738417, 2185423094, 2192483455]    # use with the Person API
}

Requesting an individual from the server:
person_id = 2373686152
request = Request_thread(f'{TOP_API_URL}/person/{person_id}')
request.start()
request.join()

Example JSON returned from the server
{
    'id': 2373686152, 
    'name': 'Stella', 
    'birth': '9-3-1846', 
    'parent_id': 5428641880,   # use with the Family API
    'family_id': 6128784944    # use with the Family API
}

You will lose 10% if you don't detail your part 1 and part 2 code below

Describe how to speed up part 1

<Add your comments here>


Describe how to speed up part 2

<Add your comments here>


Extra (Optional) 10% Bonus to speed up part 3

<Add your comments here>

"""

from common import *
import queue

def query_family(family_id):

    req = Request_thread(f'{TOP_API_URL}/family/{family_id}')
    req.start()
    req.join()

    data = req.get_response()
    # print(data)

    if data is not None:
        return data
    else:
        return None
        

def query_person(person_id):
    req = Request_thread(f'{TOP_API_URL}/person/{person_id}')
    req.start()
    req.join()

    data = req.get_response()

    if data is not None:
        return data
    else:
        return None

def display_family_details(family, husband, wife, children):
    #* Family Terminal Outputs
            print("\n-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_ Family Node _-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_")
            if family is None:
                print("Family not found.")
            else:
                print(f"Family Found: {family.get_id()}")

            if husband is None:
                print("Husband not found.")
            else:
                print(f"Husband Found: {husband.get_id()}, {husband.get_name()}")

            if wife is None:
                print("Wife not found.")
            else:
                print(f"Wife Found: {wife.get_id()}, {wife.get_name()}\n")

            if not children or len(children) <= 0:
                print("No children found.")
            else:
                print("Children Found:")
                for kid in children:
                    child = Person(query_person(kid))
                    print(f"{child.get_id()}, {child.get_name()}", {child.get_familyid()}, end=" ")

            print("\n       Moving to Next Family\n")


def move_left(family_id, tree: Tree):
    #! DON'T PROCESS EXISTING PERSONS (A PARENT IS ALSO A CHILD, CAN'T ADD THEM AS A CHILD IF THEY ARE ALREADY ADDED AS A PARENT)
    if not family_id:
        return
    
    f_req = Request_thread(f'{TOP_API_URL}/family/{family_id}')
    f_req.start()
    f_req.join()

    family = Family(f_req.get_response())
    # print(f"\nGot family: {family.get_id()}")
    tree.add_family(family)

    h_req = Request_thread(f'{TOP_API_URL}/person/{family.get_husband()}')
    h_req.start()
    h_req.join()

    husband = Person(h_req.get_response())
    # print(f"Got Husband: {husband.get_id()}, {husband.get_name()}")

    w_req = Request_thread(f'{TOP_API_URL}/person/{family.get_wife()}')
    w_req.start()
    w_req.join()

    wife = Person(w_req.get_response())
    # print(f"Got Wife: {wife.get_id()}, {wife.get_name()}")

    children = family.get_children()
    # print("Found children: ")

    for child_id in children:
        c_req = Request_thread(f'{TOP_API_URL}/person/{child_id}')
        c_req.start()
        c_req.join()

        child = Person(c_req.get_response())

        if not tree.does_person_exist(child.get_id()):
            
            # print(f"Child: {child.get_id()}, {child.get_name()}")
            tree.add_person(child)

    if husband:

        if not tree.does_person_exist(husband.get_id()):
            tree.add_person(husband)

        husband_parent_id = husband.get_parentid()

        if husband_parent_id != wife.get_id():
            paternal = threading.Thread(target=move_left, args=(husband_parent_id, tree, ))
            paternal.start()
        else:
            paternal = None
    else:
        paternal = None
            
    if wife:
        
        if not tree.does_person_exist(wife.get_id()):
            tree.add_person(wife)

        wife_parent_id = wife.get_parentid()
        maternal = threading.Thread(target=move_left, args=(wife_parent_id, tree))
        maternal.start()
    else:
        maternal = None

    if paternal:
        # paternal.start()
        paternal.join()
        
    if maternal:
        # maternal.start()
        maternal.join()



#* PART ONE -----------------------------------------------------------------------------
def depth_fs_pedigree(family_id, tree: Tree) -> None:
    """Recieves a family_id int and finds where it goes on the tree and adds it recursively."""
    # KEEP this function even if you don't implement it
    # TODO - implement Depth first retrieval
    if family_id is None:
        return
    
    f_req = Request_thread(f'{TOP_API_URL}/family/{family_id}')
    f_req.start()
    f_req.join()

    family = Family(f_req.get_response())
    # print(f"\nGot family: {family.get_id()}")
    tree.add_family(family)

    h_req = Request_thread(f'{TOP_API_URL}/person/{family.get_husband()}')
    h_req.start()
    h_req.join()

    husband = Person(h_req.get_response())

    # print(f"Got Husband: {husband.get_id()}, {husband.get_name()}")

    w_req = Request_thread(f'{TOP_API_URL}/person/{family.get_wife()}')
    w_req.start()
    w_req.join()

    wife = Person(w_req.get_response())
    # print(f"Got Wife: {wife.get_id()}, {wife.get_name()}")

    children = family.get_children()
    # print("Found children: ")

    child_threads = []

    for child_id in children:
        c_req = Request_thread(f'{TOP_API_URL}/person/{child_id}')
        c_req.start()
        c_req.join()

        child = Person(c_req.get_response())

        if not tree.does_person_exist(child.get_id()):
            
            # print(f"Child: {child.get_id()}, {child.get_name()}")
            child_threads.append(threading.Thread(target=tree.add_person, args=(child, )))

    for thread in child_threads:
        thread.start()
    
    if husband:

        if not tree.does_person_exist(husband.get_id()):
            tree.add_person(husband)

        husband_parent_id = husband.get_parentid()

        if husband_parent_id != wife.get_id():
            paternal = threading.Thread(target=depth_fs_pedigree, args=(husband_parent_id, tree, ))
            paternal.start()
        else:
            paternal = None
    else:
        paternal = None
            
    if wife:
        
        if not tree.does_person_exist(wife.get_id()):
            tree.add_person(wife)

        wife_parent_id = wife.get_parentid()
        maternal = threading.Thread(target=depth_fs_pedigree, args=(wife_parent_id, tree))
        maternal.start()
    else:
        maternal = None

    for thread in child_threads:
        thread.join()

    if paternal:
        # paternal.start()
        paternal.join()
        
    if maternal:
        # maternal.start()
        maternal.join()

    pass
    # TODO - Printing out people and families that are retrieved from the server will help debugging


#* PART ONE -----------------------------------------------------------------------------
def breadth_fs_pedigree(family_id, tree: Tree) -> None:
    """"""
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval

    fq = queue.Queue()
    fq.put(family_id)

    def helper_fun(family_id):
        """
        - No recursion
        - Adding to the queue
        - nonlocal
        """
        if family_id is None:
            return

        nonlocal fq
        nonlocal tree

        f_req = Request_thread(f'{TOP_API_URL}/family/{family_id}')
        f_req.start()
        f_req.join()


        family = Family(f_req.get_response())
        # print(f"\nGot family: {family.get_id()}")

        if not tree.does_family_exist(family_id):
            fq.put(family_id)
            # print(f"Added {family_id} family to tree.")
            tree.add_family(family)

        h_req = Request_thread(f'{TOP_API_URL}/person/{family.get_husband()}')
        h_req.start()

        w_req = Request_thread(f'{TOP_API_URL}/person/{family.get_wife()}')
        w_req.start()

        children = family.get_children()
        # print("Found children: ")

        for child_id in children:
            c_req = Request_thread(f'{TOP_API_URL}/person/{child_id}')
            c_req.start()
            c_req.join()

            child = Person(c_req.get_response())

            if not tree.does_person_exist(child.get_id()):
                
                # print(f"Child: {child.get_id()}, {child.get_name()}")
                tree.add_person(child)

        h_req.join()
        w_req.join()

        husband = Person(h_req.get_response())
        # print(f"Got Husband: {husband.get_id()}, {husband.get_name()}")

        wife = Person(w_req.get_response())
        # print(f"Got Wife: {wife.get_id()}, {wife.get_name()}")

        if husband:

            if not tree.does_person_exist(husband.get_id()):
                tree.add_person(husband)

            husband_parent_id = husband.get_parentid()
            # print(f"Got husb parent ID: {husband_parent_id}")

            if husband_parent_id != wife.get_id():
                fq.put(husband_parent_id)#!
                
        if wife:
            
            if not tree.does_person_exist(wife.get_id()):
                tree.add_person(wife)

            wife_parent_id = wife.get_parentid()
            fq.put(wife_parent_id)#!

    while not fq.empty():
        threads = []
        count = 0

        while count < 30 and not fq.empty():
            count += 1
            
            threads.append(threading.Thread(target=helper_fun, args=(fq.get(), )))
        
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()



#* 10% EXTRA BONUS (optional) -----------------------------------------------------------------------------
def breadth_fs_pedigree_limit5(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval

    #      - Limit number of concurrent connections to the FS server to 5
    # TODO - Printing out people and families that are retrieved from the server will help debugging

    pass

def troubleshoot(person_id: int) -> Family:

    person = query_person(person_id)
    family = query_family(person.get_familyid())
    return family


def main():
    
    generations = 6

    req = Request_thread(TOP_API_URL)
    req.start()
    req.join()
    
    data = req.get_response()

    start_fam_id= data['start_family_id']
    print(start_fam_id)

    start = Request_thread(f'{TOP_API_URL}/start/{generations}')
    start.start()
    start.join()

    print (start.get_response())

    tree = Tree(start_fam_id)
    
    move_left(start_fam_id, tree)


    

if __name__ == '__main__':
    main()