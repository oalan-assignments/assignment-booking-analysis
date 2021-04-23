import bisect


class Flight:
    def __init__(self, dep_time, origin, destination):
        self.dep_time = dep_time
        self.origin = origin
        self.destination = destination

class Booking:
    def __init__(self, id, flights):
        self.id = id #added id for easier tracing while trying it out
        self.flights = flights
        self.date = flights[0].dep_time

    def __lt__(self, other):
        return self.date < other

    def __repr__(self):
        return f"Booking {self.id}"


class BookingsDb:
    flight_legs_to_bookings = {}
    bookings_sorted_by_date = []

    # upper bound: O(n log n) (due to built-in Timsort)
    def add(self, booking):
        self.bookings_sorted_by_date.append(booking)
        self.bookings_sorted_by_date.sort(key=lambda x: x.date)
        for flight in booking.flights:
            leg = (flight.origin, flight.destination)
            current_list = self.flight_legs_to_bookings.get(leg)
            if current_list is None:
                self.flight_legs_to_bookings[leg] = [booking]
            else:
                current_list.append(booking)
                self.flight_legs_to_bookings[leg] = current_list

    # upper bound: O(log n) (binary search)
    def get_before(self, timestamp):
        index = bisect.bisect_left(self.bookings_sorted_by_date, timestamp)
        return self.bookings_sorted_by_date[:index]

    # upper bound: O(1)
    def get_visiting(self, origin, destination):
        return self.flight_legs_to_bookings.get((origin, destination))



db = BookingsDb()
booking1 = Booking("1", [Flight("2019-04-17T13:47:27.413Z", "AMS", "BUD"),
                         Flight("2019-03-17T13:47:27.413Z", "BUD", "AMS")])

booking2 = Booking("2", [Flight("2021-03-15T13:47:27.413Z", "AMS", "BUD"),
                         Flight("2021-03-16T13:47:27.413Z", "BUD", "LHR")])

db.add(booking1)
db.add(booking2)

print(db.get_before("2010-02-15T13:47:27.413Z")) #empty
print(db.get_before("2020-12-15T13:47:27.413Z")) #booking 1
print(db.get_before("2021-12-15T13:47:27.413Z")) #booking 1 & 2

print(db.get_visiting("NYC", "RIO")) #empty
print(db.get_visiting("BUD", "AMS")) #booking 1
print(db.get_visiting("AMS", "BUD")) #booking 1 & 2



