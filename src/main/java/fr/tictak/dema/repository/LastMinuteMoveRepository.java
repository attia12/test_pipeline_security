package fr.tictak.dema.repository;

import fr.tictak.dema.model.LastMinuteMove;

import fr.tictak.dema.model.enums.QuotationType;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface LastMinuteMoveRepository extends MongoRepository<LastMinuteMove, String> {
    List<LastMinuteMove> findByModeAndBooked(QuotationType mode, Boolean booked);
    @Query(value = "{'driver.$id': ?0}")
    void deleteByDriverId(String driverId);

}