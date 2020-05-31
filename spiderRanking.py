import sqlite3


class spiderRanking:
    con = sqlite3.connect('spider2.sqlite')
    cur = con.cursor()

    # part 1 populate form_id list
    cur.execute('select distinct from_id from Links')
    from_ids = list()
    for row in cur:
        from_ids.append(row[0])
    # print('from ids', from_ids)

    # part 2 populate to_ids and links
    cur.execute('select distinct from_id,to_id from Links')
    to_ids = list()
    links = list()
    for row in cur:
        fromIds = row[0]
        toIds = row[1]
        if fromIds == toIds:
            continue
        if fromIds not in from_ids:
            continue
        if toIds not in from_ids:
            continue
        links.append(row)
        if toIds not in to_ids: to_ids.append(toIds)
    # print('to ids', to_ids)
    # print('all unique links', links)

    # part 3 populating previous rank from pages
    prev_ranks = dict()
    for node in from_ids:
        cur.execute('select new_rank from Pages where id=?', (node,))
        row = cur.fetchone()
        prev_ranks[node] = row[0]

    many = 1
    inputInteration = input('How many interation ?')
    if len(inputInteration) > 1: many = int(inputInteration)

    # part 4 Sanity check previous rank if pages have been spidered
    if len(prev_ranks) < 1:
        print("Nothing to page rank.  Check data.")
        quit()

    # part 5 iteration as per input, DEFAULT = 1
    for i in range(many):
        total = 0.0
        next_ranks = dict()

        # part 6 put old rank plus total in next_ranks
        for node, old_rank in list(prev_ranks.items()):
            total = total + old_rank
            next_ranks[node] = 0.0

            # part 7 main ranking get amount for a node
            # Find the number of outbound links and sent the page rank down each
        for node, old_rank in list(prev_ranks.items()):
            give_ids = list()
            for from_id, to_id in links:
                if node != from_id: continue
                # if from_id == to_id: continue
                # if from_id not in from_ids: continue
                if to_id not in to_ids: continue
                give_ids.append(to_id)
            if len(give_ids) < 1: continue

            amount = old_rank / len(give_ids)

            # print('here', node, amount, give_ids)
            for id in give_ids:
                # print('ggg', id, next_ranks[id])
                next_ranks[id] = next_ranks[id] + amount

        # part 8 calculation of evap
        newtot = 0
        for node, next_rank in list(next_ranks.items()):
            newtot = newtot + next_rank
        evap = (total - newtot) / len(next_ranks)

        for node in (next_ranks):
            next_ranks[node] = next_ranks[node] + evap

        # part 9 total and average diff
        newtot = 0
        for node, next_rank in list(next_ranks.items()):
            newtot = newtot + next_rank

        totdiff = 0
        for node, old_rank in list(prev_ranks.items()):
            new_rank = next_ranks[node]
            diff = abs(old_rank - new_rank)
            totdiff = totdiff + diff

        avediff = totdiff / len(prev_ranks)
        print(i + 1, avediff)

        # rotate
        prev_ranks = next_ranks

    # Put the final ranks back into the database
    print(list(next_ranks.items())[:5])
    cur.execute('''UPDATE Pages SET old_rank=new_rank''')
    for (id, new_rank) in list(next_ranks.items()):
        cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (new_rank, id))
    con.commit()
    cur.close()

