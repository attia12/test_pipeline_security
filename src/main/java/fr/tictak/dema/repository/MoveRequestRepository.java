package fr.tictak.dema.repository;

import fr.tictak.dema.model.MoveRequest;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;
import java.util.Optional;

public interface MoveRequestRepository extends MongoRepository<MoveRequest, String> {
    Optional<MoveRequest> findByConfirmationToken(String token);
    Optional<MoveRequest> findByPhotoConfirmationToken(String token);
    Optional<MoveRequest> findByDriverIdAndAssignmentStatus(String driverId, String assignmentStatus);
    List<MoveRequest> findByAssignmentStatus(String assignmentStatus);
    List<MoveRequest> findByClientId(String userId);


    void deleteAllByClientEmail(String email);
}