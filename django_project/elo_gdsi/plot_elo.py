import sqlite3
import database_utils
from matplotlib import pyplot


def _iter_player_matches(database, player_id):
    cursor = database.cursor()
    cursor.execute((
        'select date, not_played, player1_id, player1_elo, '
        'player2_id, player2_elo '
        'from matches join elo on match_id = matches.id '
        'where player1_id == ? or player2_id == ? '
        'order by date asc'
    ), (player_id, player_id))
    for match in cursor:
        date = database_utils.get_date_from_timestamp(match[0])
        elo = match[3] if player_id == match[2] else match[5]
        match_played = not match[1]
        yield (date, match_played, elo)


def plot_player(player_id):
    db = sqlite3.Connection('gdsi.db')
    dates = []
    elos = []
    for date, match_played, elo in _iter_player_matches(db, player_id):
        if match_played:
            dates.append(date)
            elos.append(elo)

    pyplot.plot(dates, elos, markerfacecolor='r', marker='o')
    pyplot.show()


if __name__ == '__main__':
    plot_player(1282)
