package fr.tictak.dema.service.implementation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.maps.errors.ApiException;
import fr.tictak.dema.dto.in.*;
import fr.tictak.dema.dto.out.MoveDetails;
import fr.tictak.dema.exception.BadRequestException;
import fr.tictak.dema.exception.ForbiddenException;
import fr.tictak.dema.exception.ResourceNotFoundException;
import fr.tictak.dema.model.*;
import fr.tictak.dema.model.enums.MissionStatus;
import fr.tictak.dema.model.enums.QuotationStatus;
import fr.tictak.dema.model.enums.QuotationType;
import fr.tictak.dema.model.enums.Role;
import fr.tictak.dema.model.user.Admin;
import fr.tictak.dema.model.user.Driver;
import fr.tictak.dema.model.user.User;
import fr.tictak.dema.repository.*;
import fr.tictak.dema.service.MoveService;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring6.SpringTemplateEngine;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.Comparator;

@Slf4j
@Service
@Tag(name = "Service de gestion des déménagements", description = "Service pour la gestion des demandes de déménagement.")
public class MoveServiceImpl implements MoveService {

    private final MoveRequestRepository moveRequestRepository;
    private final ItemRepository itemRepository;
    private final GoogleMapsService googleMapsService;
    private final SimpMessagingTemplate messagingTemplate;
    private final UserRepository userRepository;
    private final NotificationService notificationService;
    private final TaskScheduler taskScheduler;
    private final DriverRepository driverRepository;
    private final MongoTemplate mongoTemplate;
    private final SpringTemplateEngine templateEngine;
    private final ObjectMapper objectMapper;
    private final JavaMailSender mailSender;
    private final ConcurrentHashMap<String, ScheduledFuture<?>> assignmentTimeouts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Bucket> userBuckets = new ConcurrentHashMap<>();
    private final LastMinuteMoveRepository lastMinuteMoveRepository;



    public MoveServiceImpl(MoveRequestRepository moveRequestRepository, ItemRepository itemRepository, GoogleMapsService googleMapsService, SimpMessagingTemplate messagingTemplate, UserRepository userRepository, NotificationService notificationService, TaskScheduler taskScheduler, DriverRepository driverRepository, MongoTemplate mongoTemplate, SpringTemplateEngine templateEngine, ObjectMapper objectMapper, JavaMailSender mailSender, LastMinuteMoveRepository lastMinuteMoveRepository) {
        this.moveRequestRepository = moveRequestRepository;
        this.itemRepository = itemRepository;
        this.googleMapsService = googleMapsService;
        this.messagingTemplate = messagingTemplate;
        this.userRepository = userRepository;
        this.notificationService = notificationService;
        this.taskScheduler = taskScheduler;
        this.driverRepository = driverRepository;
        this.mongoTemplate = mongoTemplate;
        this.templateEngine = templateEngine;
        this.objectMapper = objectMapper;
        this.mailSender = mailSender;
        this.lastMinuteMoveRepository = lastMinuteMoveRepository;
    }

    private Bucket getUserBucket(String email) {
        return userBuckets.computeIfAbsent(email, k -> {
            Bandwidth limit = Bandwidth.classic(20, Refill.intervally(20, Duration.ofHours(24)));
            return Bucket.builder().addLimit(limit).build();
        });
    }

    @Data
    public static class DriverLocation {
        private String driverId;
        private double latitude;
        private double longitude;

        public DriverLocation(String driverId, double latitude, double longitude) {
            this.driverId = driverId;
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }

    private final ConcurrentHashMap<String, DriverLocation> onlineDrivers = new ConcurrentHashMap<>();

    public void addOnlineDriver(String driverId) {
        onlineDrivers.putIfAbsent(driverId, new DriverLocation(driverId, 0.0, 0.0));
        log.info("Chauffeur ajouté aux chauffeurs en ligne : {}, Taille actuelle : {}", driverId, onlineDrivers.size());
    }
    public void removeOnlineDriver(String driverId) {
        onlineDrivers.remove(driverId);
        log.info("Chauffeur retiré des chauffeurs en ligne : {}, Taille actuelle : {}", driverId, onlineDrivers.size());
    //added part
        Query query = new Query();
        query.addCriteria(Criteria.where("assignmentStatus").is("WAITING_FOR_DRIVER")
                .and("candidateDrivers").in(driverId));
        List<MoveRequest> affectedMoves = mongoTemplate.find(query, MoveRequest.class);

        for (MoveRequest moveRequest : affectedMoves) {
            List<String> candidates = moveRequest.getCandidateDrivers();
            int index = candidates.indexOf(driverId);
            if (index == -1) continue;
            candidates.remove(index);
            int currentIndex = moveRequest.getCurrentDriverIndex();
            boolean wasCurrent = (index == currentIndex);
            if (index < currentIndex) {
                currentIndex--;
                moveRequest.setCurrentDriverIndex(currentIndex);
            }
            if (candidates.isEmpty() || currentIndex >= candidates.size()) {
                moveRequest.setAssignmentStatus("NO_DRIVERS_AVAILABLE");
                MissionHistory event = new MissionHistory(moveRequest.getMoveId(), "STATUS_CHANGED", LocalDateTime.now(),
                        "Assignment status changed to NO_DRIVERS_AVAILABLE due to driver disconnection", "SYSTEM");
                moveRequest.getHistoryEvents().add(event);
                moveRequestRepository.save(moveRequest);
                assignmentTimeouts.remove(moveRequest.getMoveId());
                continue;
            }
            if (wasCurrent) {
                ScheduledFuture<?> future = assignmentTimeouts.remove(moveRequest.getMoveId());
                if (future != null) {
                    future.cancel(false);
                }
                String details = String.format("Driver %s disconnected", driverId);
                MissionHistory event = new MissionHistory(moveRequest.getMoveId(), "DRIVER_DISCONNECTED", LocalDateTime.now(), details, "SYSTEM");
                moveRequest.getHistoryEvents().add(event);
                String nextDriverId = candidates.get(currentIndex);
                details = String.format("Offer sent to driver %s", nextDriverId);
                event = new MissionHistory(moveRequest.getMoveId(), "MISSION_OFFERED_TO_DRIVER", LocalDateTime.now(), details, "SYSTEM");
                moveRequest.getHistoryEvents().add(event);
                long timeoutDurationSeconds = 60;
                double distanceInKm = googleMapsService.getDistance(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
                int durationInMinutes = googleMapsService.getDurationInMinutes(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
                int totalItems = moveRequest.getItems().stream().mapToInt(ItemQuantity::getQuantity).sum();
                DecimalFormat decimalFormat = new DecimalFormat("#0.00");
                String formattedPostCommissionCost = decimalFormat.format(moveRequest.getPostCommissionCost());
                String formattedDistanceInKm = decimalFormat.format(distanceInKm);
                String formattedDurationInMinutes = String.valueOf(durationInMinutes);
                String formattedTotalItems = String.valueOf(totalItems);
                MissionNotification missionNotification = new MissionNotification(
                        moveRequest.getMoveId(),
                        moveRequest.getSourceAddress(),
                        moveRequest.getDestinationAddress(),
                        formattedPostCommissionCost,
                        formattedDurationInMinutes,
                        formattedTotalItems,
                        formattedDistanceInKm,
                        timeoutDurationSeconds,
                        "OFFERED",
                        moveRequest.getClient().getLastName() + ' ' + moveRequest.getClient().getFirstName(),
                        moveRequest.getClient().getPhoneNumber(),
                        moveRequest.getPlannedDate(),
                        moveRequest.getPlannedTime(),
                        moveRequest.getItems()
                );
                String body;
                try {
                    body = objectMapper.writeValueAsString(missionNotification);
                } catch (JsonProcessingException e) {
                    log.error("Failed to serialize MissionNotification to JSON", e);
                    continue;
                }
                Notification notification = new Notification();
                notification.setTitle("New Mission Offer");
                notification.setBody(body);
                notification.setTimestamp(LocalDateTime.now());
                notification.setRead(false);
                notification.setNotificationType("MISSION_OFFER");
                notification.setRelatedMoveId(moveRequest.getMoveId());
                notification.setUserId(nextDriverId);
                notification.setStatus("SENT");
                notificationService.sendAndSaveNotification(notification, missionNotification);
                log.info("Mission offered to next driver {} for moveId {} after disconnection", nextDriverId, moveRequest.getMoveId());
                ScheduledFuture<?> newFuture = taskScheduler.schedule(() -> checkAssignmentTimeout(moveRequest.getMoveId()),
                        new Date(System.currentTimeMillis() + timeoutDurationSeconds * 1000));
                assignmentTimeouts.put(moveRequest.getMoveId(), newFuture);
            }
            moveRequestRepository.save(moveRequest);
        }

        //fin part
    }

    public List<DriverLocation> getOnlineDrivers() {
        log.info("Récupération des chauffeurs en ligne, Taille actuelle : {}", onlineDrivers.size());
        return new ArrayList<>(onlineDrivers.values());
    }

    private static final double MAX_RADIUS_KM = 500000.0;

    public List<String> getBusyDriverIds() {
        return moveRequestRepository.findByAssignmentStatus("ASSIGNED")
                .stream()
                .map(mr -> mr.getDriver() != null ? mr.getDriver().getId() : null)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
    public void startDriverAssignment(String moveId) {
        log.info("Début du processus d'assignation de chauffeur pour moveId : {}", moveId);

        MoveRequest moveRequest = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> {
                    log.error("Demande de déménagement non trouvée pour moveId : {}", moveId);
                    return new IllegalArgumentException("Move request not found: " + moveId);
                });

        GeoJsonPoint sourceLocation = moveRequest.getSourceLocation();
        double sourceLat = sourceLocation.getY();
        double sourceLon = sourceLocation.getX();
        String originCoords = sourceLat + "," + sourceLon;

        List<String> busyDriverIds = getBusyDriverIds();
        List<DriverLocation> availableDrivers = getOnlineDrivers().stream()
                .filter(driver -> !busyDriverIds.contains(driver.getDriverId()))
                //new filter
                .filter(driver -> driver.getLatitude() != 0.0 && driver.getLongitude() != 0.0)
                .toList();

        log.info("Nombre de chauffeurs en ligne disponibles (non occupés) : {}", availableDrivers.size());

        if (availableDrivers.isEmpty()) {
            log.warn("Aucun chauffeur en ligne disponible pour moveId : {}", moveId);
            moveRequest.setAssignmentStatus("NO_DRIVERS_AVAILABLE");
            MissionHistory event = new MissionHistory(moveId, "STATUS_CHANGED", LocalDateTime.now(),
                    "Assignment status changed to NO_DRIVERS_AVAILABLE", "SYSTEM");
            moveRequest.getHistoryEvents().add(event);
            moveRequestRepository.save(moveRequest);
            return;
        }

        List<String> destinationCoords = availableDrivers.stream()
                .map(driver -> driver.getLatitude() + "," + driver.getLongitude())
                .collect(Collectors.toList());

        List<Double> drivingDistances;
        try {
            drivingDistances = googleMapsService.getDistances(originCoords, destinationCoords, moveRequest.getClientEmail());
            log.info("Distances de conduite récupérées : {}", drivingDistances);
        } catch (RuntimeException e) {
            log.error("Échec du calcul des distances pour moveId : {}. Erreur : {}", moveId, e.getMessage());
            moveRequest.setAssignmentStatus("DISTANCE_CALCULATION_FAILED");
            MissionHistory event = new MissionHistory(moveId, "STATUS_CHANGED", LocalDateTime.now(),
                    "Assignment status changed to DISTANCE_CALCULATION_FAILED", "SYSTEM");
            moveRequest.getHistoryEvents().add(event);
            moveRequestRepository.save(moveRequest);
            return;
        }

        if (drivingDistances.isEmpty() || drivingDistances.size() != availableDrivers.size()) {
            log.warn("Résultats de distance invalides pour moveId : {}", moveId);
            moveRequest.setAssignmentStatus("DISTANCE_CALCULATION_FAILED");
            MissionHistory event = new MissionHistory(moveId, "STATUS_CHANGED", LocalDateTime.now(),
                    "Assignment status changed to DISTANCE_CALCULATION_FAILED", "SYSTEM");
            moveRequest.getHistoryEvents().add(event);
            moveRequestRepository.save(moveRequest);
            return;
        }

        List<DriverDistancePair> driverDistances = new ArrayList<>();
        for (int i = 0; i < availableDrivers.size(); i++) {
            double distance = drivingDistances.get(i) / 1000.0;
            if (distance <= MAX_RADIUS_KM) {
                driverDistances.add(new DriverDistancePair(availableDrivers.get(i), distance));
            }
        }
        driverDistances.sort(Comparator.comparingDouble(pair -> pair.distance));

        List<DriverLocation> sortedDrivers = driverDistances.stream()
                .map(pair -> pair.driver)
                .toList();

        if (sortedDrivers.isEmpty()) {
            log.warn("Aucun chauffeur dans un rayon de {} km pour moveId : {}", MAX_RADIUS_KM, moveId);
            moveRequest.setAssignmentStatus("NO_DRIVERS_IN_RANGE");
            MissionHistory event = new MissionHistory(moveId, "STATUS_CHANGED", LocalDateTime.now(),
                    "Assignment status changed to NO_DRIVERS_IN_RANGE", "SYSTEM");
            moveRequest.getHistoryEvents().add(event);
            moveRequestRepository.save(moveRequest);
            return;
        }

        moveRequest.setCandidateDrivers(sortedDrivers.stream()
                .map(DriverLocation::getDriverId)
                .collect(Collectors.toList()));
        moveRequest.setCurrentDriverIndex(0);
        moveRequest.setAssignmentStatus("WAITING_FOR_DRIVER");
        String closestDriverId = sortedDrivers.getFirst().getDriverId();
        String details = String.format("Offer sent to driver %s ", closestDriverId);
        MissionHistory event = new MissionHistory(moveId, "MISSION_OFFERED_TO_DRIVER", LocalDateTime.now(), details, "SYSTEM");
        moveRequest.getHistoryEvents().add(event);
        moveRequestRepository.save(moveRequest);

        long timeoutDurationSeconds = 60;

        int stairTimeTotalSeconds = 0;
        int totalFloors = moveRequest.getSourceFloors() + moveRequest.getDestinationFloors();

        for (ItemQuantity iq : moveRequest.getItems()) {
            int itemStairTimeSeconds;
            if (totalFloors > 0) {
                itemStairTimeSeconds = iq.getStairTime() * totalFloors;
            } else {
                itemStairTimeSeconds = iq.getStairTime();
            }

            log.info("📝 Item: {} | stairs : {} | StairTime per item (sec): {} ",
                    iq.getItemLabel(), totalFloors, itemStairTimeSeconds);

            stairTimeTotalSeconds += itemStairTimeSeconds;
        }

        double stairTimeTotalMinutes = stairTimeTotalSeconds / 60.0;
        log.info("🧮 Total stair time for all items: {} minutes", stairTimeTotalMinutes);

        int riskMarginMinutes = 15;

        double distanceInKm = googleMapsService.getDistance(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
        int durationInMinutes = googleMapsService.getDurationInMinutes(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
        int totalItems = moveRequest.getItems().stream().mapToInt(ItemQuantity::getQuantity).sum();
        int estimatedTotalMinutes = durationInMinutes + (int) Math.ceil(stairTimeTotalSeconds / 60.0) + riskMarginMinutes;
        moveRequest.setEstimatedTotalMinutes(estimatedTotalMinutes);
        moveRequestRepository.save(moveRequest);

        log.info("⏳ Estimated total minutes = {}", estimatedTotalMinutes);

        DecimalFormat decimalFormat = new DecimalFormat("#0.00");
        String formattedPostCommissionCost = decimalFormat.format(moveRequest.getPostCommissionCost());
        String formattedDistanceInKm = decimalFormat.format(distanceInKm);
        String formattedDurationInMinutes = String.valueOf(durationInMinutes);
        String formattedTotalItems = String.valueOf(totalItems);

        MissionNotification missionNotification = new MissionNotification(
                moveRequest.getMoveId(),
                moveRequest.getSourceAddress(),
                moveRequest.getDestinationAddress(),
                formattedPostCommissionCost,
                formattedDurationInMinutes,
                formattedTotalItems,
                formattedDistanceInKm,
                timeoutDurationSeconds,
                "OFFERED",
                moveRequest.getClient().getLastName() + ' ' + moveRequest.getClient().getFirstName(),
                moveRequest.getClient().getPhoneNumber(),
                moveRequest.getPlannedDate(),
                moveRequest.getPlannedTime(),
                moveRequest.getItems());

        String body;
        try {
            body = objectMapper.writeValueAsString(missionNotification);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize MissionNotification to JSON", e);
            throw new RuntimeException("Failed to create notification", e);
        }

        Notification notification = new Notification();
        notification.setTitle("New Mission Offer");
        notification.setBody(body);
        notification.setTimestamp(LocalDateTime.now());
        notification.setRead(false);
        notification.setNotificationType("MISSION_OFFER");
        notification.setRelatedMoveId(moveId);
        notification.setUserId(closestDriverId);
        notification.setStatus("SENT");

        notificationService.sendAndSaveNotification(notification, missionNotification);
        log.info("Mission offerte au chauffeur le plus proche avec driverId : {} pour moveId : {}", closestDriverId, moveId);

        ScheduledFuture<?> future = taskScheduler.schedule(() -> checkAssignmentTimeout(moveId),
                new Date(System.currentTimeMillis() + timeoutDurationSeconds * 1000));
        assignmentTimeouts.put(moveId, future);
    }

    public void reStartDriverAssignment(String moveId) {
        MoveRequest moveRequest = moveRequestRepository.findById(moveId).orElseThrow(() -> {
            log.error("Demande de déménagement non trouvée pour moveId  : {}", moveId);
            return new IllegalArgumentException("Move request not found: " + moveId);
        });

        int currentIndex = moveRequest.getCurrentDriverIndex();
        List<String> candidates = moveRequest.getCandidateDrivers();

        ScheduledFuture<?> existingFuture = assignmentTimeouts.remove(moveId);
        if (existingFuture != null) {
            existingFuture.cancel(false);
        }

        if (currentIndex + 1 < candidates.size()) {
            String currentDriverId = candidates.get(currentIndex);
            sendMissionExpiredNotification(currentDriverId, moveRequest);

            moveRequest.setCurrentDriverIndex(currentIndex + 1);
            moveRequest.setAssignmentStatus("WAITING_FOR_DRIVER");
            String nextDriverId = candidates.get(currentIndex + 1);
            String details = String.format("Offer sent to driver %s", nextDriverId);
            MissionHistory event = new MissionHistory(moveId, "MISSION_OFFERED_TO_DRIVER", LocalDateTime.now(), details, "SYSTEM");
            moveRequest.getHistoryEvents().add(event);
            moveRequestRepository.save(moveRequest);

            long timeoutDurationSeconds = 60;

            // Create MissionNotification and Notification
            double distanceInKm = googleMapsService.getDistance(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
            int durationInMinutes = googleMapsService.getDurationInMinutes(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
            int totalItems = moveRequest.getItems().stream().mapToInt(ItemQuantity::getQuantity).sum();

            DecimalFormat decimalFormat = new DecimalFormat("#0.00");
            String formattedPostCommissionCost = decimalFormat.format(moveRequest.getPostCommissionCost());
            String formattedDistanceInKm = decimalFormat.format(distanceInKm);
            String formattedDurationInMinutes = String.valueOf(durationInMinutes);
            String formattedTotalItems = String.valueOf(totalItems);

            MissionNotification missionNotification = new MissionNotification(
                    moveRequest.getMoveId(),
                    moveRequest.getSourceAddress(),
                    moveRequest.getDestinationAddress(),
                    formattedPostCommissionCost,
                    formattedDurationInMinutes,
                    formattedTotalItems,
                    formattedDistanceInKm,
                    timeoutDurationSeconds,
                    "OFFERED",
                    moveRequest.getClient().getLastName() + ' ' + moveRequest.getClient().getFirstName(),
                    moveRequest.getClient().getPhoneNumber(),
                    moveRequest.getPlannedDate(),
                    moveRequest.getPlannedTime(),
                    moveRequest.getItems()
            );

            String body;
            try {
                body = objectMapper.writeValueAsString(missionNotification);
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize MissionNotification to JSON", e);
                throw new RuntimeException("Failed to create notification", e);
            }

            Notification notification = new Notification();
            notification.setTitle("New Mission Offer");
            notification.setBody(body);
            notification.setTimestamp(LocalDateTime.now());
            notification.setRead(false);
            notification.setNotificationType("MISSION_OFFER");
            notification.setRelatedMoveId(moveId);
            notification.setUserId(nextDriverId);
            notification.setStatus("SENT");

            notificationService.sendAndSaveNotification(notification, missionNotification);
            log.info("Mission réassignée au chauffeur suivant avec driverId : {} pour moveId : {}", nextDriverId, moveId);

            ScheduledFuture<?> newFuture = taskScheduler.schedule(() -> checkAssignmentTimeout(moveId),
                    new Date(System.currentTimeMillis() + timeoutDurationSeconds * 1000));
            assignmentTimeouts.put(moveId, newFuture);
        } else {
            log.warn("Aucun chauffeur disponible dans un rayon de {} km pour moveId : {}", MAX_RADIUS_KM, moveId);
            moveRequest.setAssignmentStatus("NO_DRIVERS_IN_RANGE");
            MissionHistory event = new MissionHistory(moveId, "STATUS_CHANGED", LocalDateTime.now(), "Assignment status changed to NO_DRIVERS_IN_RANGE", "SYSTEM");
            moveRequest.getHistoryEvents().add(event);
            moveRequestRepository.save(moveRequest);

            String lastDriverId = candidates.get(currentIndex);
            // Notify the last driver of no available drivers
            double distanceInKm = googleMapsService.getDistance(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
            int durationInMinutes = googleMapsService.getDurationInMinutes(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
            int totalItems = moveRequest.getItems().stream().mapToInt(ItemQuantity::getQuantity).sum();

            DecimalFormat decimalFormat = new DecimalFormat("#0.00");
            String formattedPostCommissionCost = decimalFormat.format(moveRequest.getPostCommissionCost());
            String formattedDistanceInKm = decimalFormat.format(distanceInKm);
            String formattedDurationInMinutes = String.valueOf(durationInMinutes);
            String formattedTotalItems = String.valueOf(totalItems);

            MissionNotification missionNotification = new MissionNotification(
                    moveRequest.getMoveId(),
                    moveRequest.getSourceAddress(),
                    moveRequest.getDestinationAddress(),
                    formattedPostCommissionCost,
                    formattedDurationInMinutes,
                    formattedTotalItems,
                    formattedDistanceInKm,
                    0,
                    "NO_DRIVERS_IN_RANGE",
                    moveRequest.getClient().getLastName() + ' ' + moveRequest.getClient().getFirstName(),
                    moveRequest.getClient().getPhoneNumber(),
                    moveRequest.getPlannedDate(),
                    moveRequest.getPlannedTime(),
                    moveRequest.getItems()
            );

            String body;
            try {
                body = objectMapper.writeValueAsString(missionNotification);
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize MissionNotification to JSON", e);
                throw new RuntimeException("Failed to create notification", e);
            }

            Notification notification = new Notification();
            notification.setTitle("No Drivers Available");
            notification.setBody(body);
            notification.setTimestamp(LocalDateTime.now());
            notification.setRead(false);
            notification.setNotificationType("NO_DRIVERS_IN_RANGE");
            notification.setRelatedMoveId(moveId);
            notification.setUserId(lastDriverId);
            notification.setStatus("SENT");

            notificationService.sendAndSaveNotification(notification, missionNotification);
        }
    }

    private void sendMissionExpiredNotification(String driverId, MoveRequest moveRequest) {
        double distanceInKm = googleMapsService.getDistance(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
        int durationInMinutes = googleMapsService.getDurationInMinutes(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
        int totalItems = moveRequest.getItems().stream().mapToInt(ItemQuantity::getQuantity).sum();

        DecimalFormat decimalFormat = new DecimalFormat("#0.00");
        String formattedPostCommissionCost = decimalFormat.format(moveRequest.getPostCommissionCost());
        String formattedDistanceInKm = decimalFormat.format(distanceInKm);
        String formattedDurationInMinutes = String.valueOf(durationInMinutes);
        String formattedTotalItems = String.valueOf(totalItems);

        MissionNotification updatedNotification = new MissionNotification(
                moveRequest.getMoveId(),
                moveRequest.getSourceAddress(),
                moveRequest.getDestinationAddress(),
                formattedPostCommissionCost,
                formattedDurationInMinutes,
                formattedTotalItems,
                formattedDistanceInKm,
                0,
                "EXPIRED",
                moveRequest.getClient().getLastName() + ' ' + moveRequest.getClient().getFirstName(),
                moveRequest.getClient().getPhoneNumber(),
                moveRequest.getPlannedDate(),
                moveRequest.getPlannedTime(),
                moveRequest.getItems()
        );

        String body;
        try {
            body = objectMapper.writeValueAsString(updatedNotification);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize MissionNotification to JSON", e);
            throw new RuntimeException("Failed to create notification", e);
        }

        Notification notification = new Notification();
        notification.setTitle("Mission Offer Expired");
        notification.setBody(body);
        notification.setTimestamp(LocalDateTime.now());
        notification.setRead(false);
        notification.setNotificationType("MISSION_EXPIRED");
        notification.setRelatedMoveId(moveRequest.getMoveId());
        notification.setUserId(driverId);
        notification.setStatus("SENT");

        notificationService.sendAndSaveNotification(notification, updatedNotification);
        log.info("Notification mise à jour en EXPIRÉ pour driverId : {} et moveId : {}", driverId, moveRequest.getMoveId());
    }

    @Override
    public MoveDetails getMoveDetails(String moveId) {
        MoveRequest moveRequest = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> new RuntimeException("Move not found: " + moveId));

        double distance = googleMapsService.getDistance(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
        int durationMinutes = googleMapsService.getDurationInMinutes(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());

        LocalDateTime departureTime = moveRequest.getAssignmentTimestamp() != null
                ? moveRequest.getAssignmentTimestamp()
                : LocalDateTime.now();
        LocalDate date = departureTime.toLocalDate();
        LocalDateTime arrivalTime = departureTime.plusMinutes(durationMinutes);

        return new MoveDetails(
                distance,
                durationMinutes,
                date,
                departureTime,
                arrivalTime,
                moveRequest.getItems(),
                moveRequest.getPaymentStatus(),
                moveRequest.getSourceAddress(),
                moveRequest.getDestinationAddress()
        );
    }

    @Override
    public List<DriverMissionSummaryDTO> getLatestMissionSummaryForClient(String clientId) {
        log.info("Fetching latest mission summary for clientId: {}", clientId);

        // Query for the latest mission where the client is involved
        Query query = new Query();
        query.addCriteria(Criteria.where("client.id").is(clientId));
        query.with(org.springframework.data.domain.Sort.by(
                org.springframework.data.domain.Sort.Order.desc("assignmentTimestamp")
        ));
        List<MoveRequest> moveRequests = mongoTemplate.find(query, MoveRequest.class);

        if (moveRequests.isEmpty()) {
            log.info("No mission found for clientId: {}", clientId);
            return Collections.emptyList();
        }

        // Transform MoveRequests into DriverMissionSummaryDTOs
        List<DriverMissionSummaryDTO> summaries = new ArrayList<>();
        for (MoveRequest moveRequest : moveRequests) {
            DriverMissionSummaryDTO summary = new DriverMissionSummaryDTO();
            summary.setMoveId(moveRequest.getMoveId());

            // Calculate duration
            int durationMinutes = googleMapsService.getDurationInMinutes(
                    moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
            String duration = String.format("%dh%02d", durationMinutes / 60, durationMinutes % 60);
            summary.setDuration(duration);

            // Set distance
            double distanceKm = googleMapsService.getDistance(
                    moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
            summary.setDistanceKm(distanceKm);

            // Set source address
            summary.setSourceAddress(moveRequest.getSourceAddress());
            summary.setDestinationAddress(moveRequest.getDestinationAddress());
            summary.setDate(moveRequest.getPlannedDate() != null
                    ? moveRequest.getPlannedDate()
                    :null);

            // Calculate product count
            int productCount = moveRequest.getItems() != null
                    ? moveRequest.getItems().stream()
                    .mapToInt(ItemQuantity::getQuantity)
                    .sum()
                    : 0;
            summary.setProductCount(productCount);

            // Set amount (pre-commission cost for clients)
            summary.setAmount(moveRequest.getPreCommissionCost());

            // Set status (use missionStatus if available, otherwise fall back to quotation status)
            summary.setStatus(moveRequest.getMissionStatus() != null
                    ? moveRequest.getMissionStatus().getNameValue()
                    : moveRequest.getStatus() != null
                    ? moveRequest.getStatus().name()
                    : "");

            // Handle driver-related fields (set to empty strings if no driver is assigned)
            if (moveRequest.getDriver() != null) {
                summary.setDriverFullName(moveRequest.getDriver().getFirstName() + " " + moveRequest.getDriver().getLastName());
                summary.setDriverPhoneNumber(moveRequest.getDriver().getPhoneNumber());
            } else {
                summary.setDriverFullName("");
                summary.setDriverPhoneNumber("");
                log.info("No driver assigned for moveId: {}", moveRequest.getMoveId());
            }

            summaries.add(summary);
        }

        log.info("Mission summaries retrieved successfully for clientId: {}", clientId);
        return summaries;
    }

    public void handleDriverAcceptance(String moveId, String driverId) {
        MoveRequest moveRequest = moveRequestRepository.findById(moveId).orElseThrow(() -> {
            log.error("Demande de déménagement non trouvée : {}", moveId);
            return new IllegalArgumentException("Move request not found: " + moveId);
        });

        Optional<MoveRequest> existingAssignments = moveRequestRepository.findByDriverIdAndAssignmentStatus(driverId, "ASSIGNED");
        if (existingAssignments.isPresent()) {
            log.warn("Le chauffeur {} est déjà assigné à une ou plusieurs missions", driverId);
            throw new IllegalStateException("Driver is already assigned to another mission");
        }

        if ("WAITING_FOR_DRIVER".equals(moveRequest.getAssignmentStatus()) &&
                moveRequest.getCandidateDrivers().get(moveRequest.getCurrentDriverIndex()).equals(driverId)) {
            moveRequest.setAssignmentStatus("ASSIGNED");
            moveRequest.setDriver(driverRepository.findById(driverId).orElseThrow(() -> {
                log.error("Chauffeur non trouvé : {}", driverId);
                return new IllegalArgumentException("Driver not found: " + driverId);
            }));
            moveRequest.setMissionStatus(MissionStatus.ACCEPTED); // Set missionStatus to ACCEPTED
            moveRequest.setAcceptedAt(LocalDateTime.now());
            String details = String.format("Accepted by driver %s", driverId);
            MissionHistory event = new MissionHistory(moveId, "MISSION_ACCEPTED", LocalDateTime.now(), details, driverId);
            moveRequest.getHistoryEvents().add(event);
            moveRequestRepository.save(moveRequest);
            log.info("Mission acceptée par le chauffeur avec driverId : {} pour moveId : {}", driverId, moveId);

            ScheduledFuture<?> future = assignmentTimeouts.remove(moveId);
            if (future != null) {
                future.cancel(false);
            }

            User client = moveRequest.getClient();
            if (client != null) {
                // Notify client about mission acceptance
                Notification clientNotification = new Notification();
                clientNotification.setNotificationType("MISSION_STATUS_UPDATE");
                clientNotification.setTimestamp(LocalDateTime.now());
                clientNotification.setRead(false);
                clientNotification.setRelatedMoveId(moveId);
                clientNotification.setUserId(client.getId());
                clientNotification.setStatus("SENT");
                notificationService.sendAndSaveNotification(clientNotification, moveRequest);
            } else {
                log.warn("Aucun client associé au moveId : {}", moveId);
            }

            // Send email to driver with mission details
            try {
                User driver = userRepository.findById(driverId).orElseThrow(() -> new ResourceNotFoundException("Chauffeur non trouvé: " + driverId));
                String driverEmail = driver.getEmail();

                Context context = new Context();
                context.setVariable("source", moveRequest.getSourceAddress());
                context.setVariable("destination", moveRequest.getDestinationAddress());
                context.setVariable("clientEmail", moveRequest.getClientEmail());
                context.setVariable("clientPhone", client != null ? client.getPhoneNumber() : "N/A");
                DecimalFormat decimalFormat = new DecimalFormat("#0.000");
                String formattedCost = decimalFormat.format(moveRequest.getPreCommissionCost());
                context.setVariable("preCommissionCost", formattedCost);
                String htmlContent = templateEngine.process("driver-mission-details", context);

                MimeMessage mimeMessage = mailSender.createMimeMessage();
                MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");
                helper.setFrom("no-reply@dema.com");  // Adjust as needed
                helper.setTo(driverEmail);
                helper.setSubject("Détails de la mission acceptée");
                helper.setText(htmlContent, true);
                mailSender.send(mimeMessage);

                log.info("Email with mission details sent successfully to driver {} for moveId: {}", driverId, moveId);
            } catch (MessagingException e) {
                log.error("Failed to send mission details email to driver {} for moveId: {}. Error: {}", driverId, moveId, e.getMessage());
                // Do not throw to prevent data loss; log and continue
            } catch (Exception e) {
                log.error("Unexpected error while sending mission details email for moveId: {}. Error: {}", moveId, e.getMessage());
                // Do not throw to guarantee no data loss on exception
            }
        } else {
            log.warn("Impossible d'accepter la mission pour moveId : {} par driverId : {}", moveId, driverId);
            throw new IllegalStateException("Cannot accept mission");
        }
    }

    private void checkAssignmentTimeout(String moveId) {
        MoveRequest moveRequest = moveRequestRepository.findById(moveId).orElse(null);
        if (moveRequest != null && "WAITING_FOR_DRIVER".equals(moveRequest.getAssignmentStatus())) {
            log.info("Le chauffeur n'a pas répondu dans la minute pour moveId : {}", moveId);
            String currentDriverId = moveRequest.getCandidateDrivers().get(moveRequest.getCurrentDriverIndex());
            String details = String.format("Timeout for driver %s", currentDriverId);
            MissionHistory event = new MissionHistory(moveId, "MISSION_TIMEOUT", LocalDateTime.now(), details, "SYSTEM");
            moveRequest.getHistoryEvents().add(event);
            moveRequestRepository.save(moveRequest);
            assignmentTimeouts.remove(moveId);
            reStartDriverAssignment(moveId);
        }
    }

    public void handleDriverDecline(String moveId, String driverId) {
        MoveRequest moveRequest = moveRequestRepository.findById(moveId).orElseThrow(() -> {
            log.error("Demande de déménagement non trouvée pour moveId: {}", moveId);
            return new IllegalArgumentException("Move request not found: " + moveId);
        });

        if (!moveRequest.getCandidateDrivers().get(moveRequest.getCurrentDriverIndex()).equals(driverId)) {
            log.warn("Le chauffeur {} n'est pas le candidat actuel pour moveId : {}", driverId, moveId);
            throw new IllegalStateException("You are not the assigned driver for this mission.");
        }

        if (!"WAITING_FOR_DRIVER".equals(moveRequest.getAssignmentStatus())) {
            log.warn("Impossible de refuser la mission pour moveId : {} par driverId : {}. Statut actuel : {}",
                    moveId, driverId, moveRequest.getAssignmentStatus());
            throw new IllegalStateException("Mission cannot be declined. Current status: " +
                    moveRequest.getAssignmentStatus());
        }

        log.info("Mission refusée par le chauffeur avec driverId : {} pour moveId : {}", driverId, moveId);
        String details = String.format("Declined by driver %s", driverId);
        MissionHistory event = new MissionHistory(moveId, "MISSION_REFUSED", LocalDateTime.now(), details, driverId);
        moveRequest.getHistoryEvents().add(event);
        moveRequestRepository.save(moveRequest);
        reStartDriverAssignment(moveId);
    }

    private static class DriverDistancePair {
        DriverLocation driver;
        double distance;

        DriverDistancePair(DriverLocation driver, double distance) {
            this.driver = driver;
            this.distance = distance;
        }
    }

    public void updateDriverLocation(String driverId, double latitude, double longitude) {
        DriverLocation dl = onlineDrivers.get(driverId);
        if (dl != null) {
            dl.setLatitude(latitude);
            dl.setLongitude(longitude);
            log.info("Mise à jour de l'emplacement pour le chauffeur avec driverId : {} à ({}, {})", driverId, latitude, longitude);
        } else {
            DriverLocation newDl = new DriverLocation(driverId, latitude, longitude);
            onlineDrivers.put(driverId, newDl);
            log.info("Ajout d'un nouveau chauffeur avec driverId : {} avec emplacement ({}, {})", driverId, latitude, longitude);
        }

        // Retry assignment for pending paid missions with no available drivers, sorted by dateOfPayment ASC (oldest first for "first paid → first delivered")
        Query pendingQuery = new Query();
        pendingQuery.addCriteria(Criteria.where("assignmentStatus")
                .in("NO_DRIVERS_AVAILABLE", "NO_DRIVERS_IN_RANGE")
                .and("paymentStatus").is("paid"));
        pendingQuery.with(Sort.by(Sort.Direction.DESC, "dateOfPayment"));
        List<MoveRequest> pendingMoves = mongoTemplate.find(pendingQuery, MoveRequest.class);

        for (MoveRequest pending : pendingMoves) {
            startDriverAssignment(pending.getMoveId());
        }
    }

    @Override
    public MoveRequest getMoveRequestById(String moveId) {
        log.info("Récupération de la demande de déménagement avec moveId : {}", moveId);
        return moveRequestRepository.findById(moveId)
                .orElseThrow(() -> {
                    logResourceNotFound(moveId);
                    return new ResourceNotFoundException("Move request not found: " + moveId);
                });
    }

    @Override
    public List<String> getAddressSuggestions(String query) {
        log.info("Récupération des suggestions d'adresses pour la requête : {}", query);
        List<String> suggestions = googleMapsService.getAddressSuggestions(query);
        log.info("Suggestions d'adresses récupérées : {} résultats", suggestions.size());
        return suggestions;
    }

    public MoveRequest calculateQuote(QuoteCalculationRequest request, String email) {
        log.info("Calcul du devis pour l'email : {}", email);

        // Check rate limit
        Bucket bucket = getUserBucket(email);
        if (!bucket.tryConsume(1)) {
            log.warn("Rate limit exceeded for user: {}. Limit: 20 requests per 24 hours.", email);
            throw new BadRequestException("Vous avez dépassé la limite de 20 demandes de devis par 24 heures. Veuillez réessayer plus tard.");
        }

        if (request.sourceAddress() == null || request.destinationAddress() == null ||
                request.items() == null || request.items().isEmpty() || request.mode() == null) {
            log.warn("Échec du calcul du devis : Champs requis manquants");
            throw new IllegalArgumentException("Missing required fields.");
        }

        List<Item> items = validateAndFetchItems(request.items());
        double distanceKm = googleMapsService.getDistance(request.sourceAddress(), request.destinationAddress(), email);
        GeoJsonPoint sourceLocation = googleMapsService.getCoordinates(request.sourceAddress(), email);
        int totalFloors = request.sourceFloors() + request.destinationFloors();
        double totalVolume = 0.0;
        int maxMinTruckSize = 0;
        int totalItems = 0;
        Map<String, Integer> chosenItems = new HashMap<>();

        // List to hold updated items with stairTime
        List<ItemQuantity> updatedItems = new ArrayList<>();

        for (ItemQuantity iq : request.items()) {
            String key = iq.getItemLabel();
            Item item = items.stream().filter(i -> i.getLabel().equals(key)).findFirst().orElseThrow();

            try {
                totalVolume += Double.parseDouble(item.getVolume()) * iq.getQuantity();
            } catch (NumberFormatException e) {
                log.error("Invalid volume format for item: {}", item.getLabel(), e);
                throw new IllegalArgumentException("Invalid item volume: " + item.getVolume());
            }

            if (iq.getQuantity() > 0) {
                maxMinTruckSize = Math.max(maxMinTruckSize, item.getMinTruckSize());
            }

            // Compute stairTime for this item
            double stairTime;
            log.info("📝 Processing item: {}", item.getLabel());
            log.info("📦 Item quantity: {}", iq.getQuantity());
            log.info("⏱️ Item stairTime (raw string): '{}'", item.getStaiTime());

            try {
                if (item.getStaiTime() != null && !item.getStaiTime().isEmpty()) {
                    double parsedStairTime = Double.parseDouble(item.getStaiTime());
                    log.info("✅ Parsed stairTime: {}", parsedStairTime);

                    stairTime = parsedStairTime * iq.getQuantity();
                    iq.setStairTime((int) stairTime);
                    log.info("Total stairTime for this item: {} (parsedStairTime * quantity)", stairTime);
                } else {
                    log.warn("⚠️ StairTime is null or empty for item: {}", item.getLabel());
                }
            } catch (NumberFormatException e) {
                log.error("❌ Invalid stairTime format for item: {}", item.getLabel(), e);
                throw new IllegalArgumentException("Invalid item stairTime: " + item.getStaiTime());
            }

            totalItems += iq.getQuantity();
            chosenItems.put(key, iq.getQuantity());

            updatedItems.add(iq); // Add to updated list
        }

        class Truck {
            final int volume;
            final double price;

            Truck(int volume, double price) {
                this.volume = volume;
                this.price = price;
            }
        }

        List<Truck> truckTypes = Arrays.asList(
                new Truck(12, 107.91),
                new Truck(20, 129.7)
        );

        int minTruckVolume = truckTypes.stream().mapToInt(t -> t.volume).min().orElse(0);
        int maxTruckVolume = truckTypes.stream().mapToInt(t -> t.volume).max().orElse(0);

        if (maxMinTruckSize > maxTruckVolume || maxMinTruckSize < minTruckVolume) {
            throw new IllegalArgumentException("Selected items have truck size requirements incompatible with available trucks (12 m³ or 20 m³). Consider adjusting item selection.");
        }

        double basePrice;
        if (totalVolume == 0) {
            basePrice = 0;
        } else if (maxMinTruckSize > 12) {
            int numLarge = (int) Math.ceil(totalVolume / 20.0);
            basePrice = numLarge * 129.7;
        } else {
            double minCost = Double.MAX_VALUE;
            int maxLarge = (int) Math.ceil(totalVolume / 12.0);
            for (int numLarge = 0; numLarge <= maxLarge; numLarge++) {
                double remainingVolume = Math.max(0, totalVolume - numLarge * 20.0);
                int numSmall = (int) Math.ceil(remainingVolume / 12.0);
                double cost = numLarge * 129.7 + numSmall * 107.91;
                if (cost < minCost) minCost = cost;
            }
            basePrice = minCost;
        }

        double handlingCost;
        double totalStairTimeSeconds = 0.0;
        for (ItemQuantity iq : updatedItems) {
            totalStairTimeSeconds += iq.getStairTime();
        }
        double handlingMinutesPerFloor = totalStairTimeSeconds / 60.0;

        if (totalFloors == 0 && totalItems <= 9) {
            handlingCost = 0;
        } else {
            double rate;
            if (totalItems <= 5) {
                rate = 2.5;
            } else if (totalItems <= 10) {
                rate = 3.5;
            } else if (totalItems <= 15) {
                rate = 4.5;
            } else if (totalItems <= 20) {
                rate = 6.0;
            } else if (totalItems <= 25) {
                rate = 7.0;
            } else {
                rate = 7.0 + Math.ceil((totalItems - 25) / 5.0);
            }
            int effectiveFloors = totalFloors > 0 ? totalFloors : 1;
            handlingCost = rate * handlingMinutesPerFloor * effectiveFloors;
        }

        double urgencyMultiplier = 1.0;
        double totalPrice = (basePrice + handlingCost) * urgencyMultiplier;
        if (distanceKm > 20) totalPrice += 2.6 * distanceKm;
        Admin admin = (Admin) userRepository.findByRole(Role.ADMIN).getFirst();
        double commissionRate = admin.getCommissionRate();
        double postCommissionCost = totalPrice * (1 - commissionRate);

        MoveRequest moveRequest = new MoveRequest();
        moveRequest.setMoveId(UUID.randomUUID().toString());
        moveRequest.setSourceAddress(request.sourceAddress());
        moveRequest.setDestinationAddress(request.destinationAddress());
        moveRequest.setSourceLocation(sourceLocation);
        moveRequest.setSourceFloors(request.sourceFloors());
        moveRequest.setSourceElevator(request.sourceElevator());
        moveRequest.setDestinationFloors(request.destinationFloors());
        moveRequest.setDestinationElevator(request.destinationElevator());
        moveRequest.setItems(updatedItems); // ✅ Use updated items with stairTime
        moveRequest.setMode(request.mode());
        moveRequest.setStatus(QuotationStatus.PENDING);
        moveRequest.setPreCommissionCost(totalPrice);
        moveRequest.setPostCommissionCost(postCommissionCost);
        moveRequest.setClientEmail(email);
        moveRequest.setClient(userRepository.findByEmail(email).orElse(null));
        moveRequest.setConfirmationToken(UUID.randomUUID().toString());
        moveRequest.setConfirmationTokenExpiry(LocalDateTime.now().plusHours(24));
        moveRequest.setPaymentStatus("pending");
        moveRequest.setPlannedDate(request.plannedDate());
        moveRequest.setPlannedTime(request.plannedTime());

        MissionHistory event = new MissionHistory(moveRequest.getMoveId(), "MISSION_CREATED", LocalDateTime.now(),
                "Mission created with cost: " + totalPrice + (request.plannedDate() != null && request.plannedTime() != null ? " Planned for: " + request.plannedDate() + " at " + request.plannedTime() : ""),
                "SYSTEM");
        moveRequest.getHistoryEvents().add(event);

        int stairTimeTotalSeconds = 0;
        for (ItemQuantity iq : updatedItems) {
            int itemStairTimeSeconds;
            if (totalFloors > 0) {
                itemStairTimeSeconds = iq.getStairTime() * totalFloors;
            } else {
                itemStairTimeSeconds = iq.getStairTime();
            }
            stairTimeTotalSeconds += itemStairTimeSeconds;
        }

        int riskMarginMinutes = 15;
        int durationInMinutes = googleMapsService.getDurationInMinutes(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
        int estimatedTotalMinutes = durationInMinutes + (int) Math.ceil(stairTimeTotalSeconds / 60.0) + riskMarginMinutes;
        moveRequest.setEstimatedTotalMinutes(estimatedTotalMinutes);

        List<String> busyDriverIds = getBusyDriverIds();
        List<DriverLocation> availableDrivers = getOnlineDrivers().stream()
                .filter(driver -> !busyDriverIds.contains(driver.getDriverId()))
                .filter(driver -> driver.getLatitude() != 0.0 && driver.getLongitude() != 0.0)
                .toList();
        if (!availableDrivers.isEmpty()) {
            moveRequest.setWaitingTime(0);
        } else {
            if (busyDriverIds.isEmpty()) {
                moveRequest.setWaitingTime(0); // No busy drivers, no estimate possible
            } else {
                long minWaitingTime = Long.MAX_VALUE;
                for (String busyDriverId : busyDriverIds) {
                    // Fetch the current assigned mission for the busy driver (assumes one active mission per driver)
                    Query busyQuery = new Query();
                    busyQuery.addCriteria(Criteria.where("driver.$id").is(new ObjectId(busyDriverId))
                            .and("assignmentStatus").is("ASSIGNED"));
                    MoveRequest busyMission = mongoTemplate.findOne(busyQuery, MoveRequest.class);
                    if (busyMission != null) {
                        // Use full mission duration
                        long missionDurationMin = busyMission.getEstimatedTotalMinutes();
                        // Calculate travel time from busy mission's destination to new mission's source
                        int travelMin = googleMapsService.getDurationInMinutes(
                                busyMission.getDestinationAddress(),
                                moveRequest.getSourceAddress(),
                                moveRequest.getClientEmail()
                        );
                        // Total wait: mission duration + travel + 60min buffer
                        long totalWaitMin = missionDurationMin + travelMin + 60;
                        if (totalWaitMin < minWaitingTime) {
                            minWaitingTime = totalWaitMin;
                        }
                    }
                }
                // Add pending missions' total duration
                Query pendingQuery = new Query();
                pendingQuery.addCriteria(Criteria.where("assignmentStatus")
                        .in("NO_DRIVERS_AVAILABLE", "NO_DRIVERS_IN_RANGE")
                        .and("paymentStatus").is("paid"));
                List<MoveRequest> pendingMoves = mongoTemplate.find(pendingQuery, MoveRequest.class);
                long pendingTotalMin = pendingMoves.stream()
                        .mapToLong(MoveRequest::getEstimatedTotalMinutes)
                        .sum();
                // For approximation, add pending total divided by number of busy drivers (assuming load balancing)
                if (!busyDriverIds.isEmpty()) {
                    long extraPerDriver = (pendingTotalMin + busyDriverIds.size() - 1) / busyDriverIds.size();
                    minWaitingTime += extraPerDriver;
                }
                moveRequest.setWaitingTime(minWaitingTime != Long.MAX_VALUE ? (int) minWaitingTime : 0);
            }
        }
// Save updated MoveRequest with waitingTime
        moveRequestRepository.save(moveRequest);
        log.info("Devis calculé avec succès pour moveId : {}", moveRequest.getMoveId());
        return moveRequest;
    }

    @Override
    public List<MoveRequest> getAllMoveRequests() {
        log.info("Récupération de toutes les demandes de déménagement");
        List<MoveRequest> moves = moveRequestRepository.findAll();
        log.info("Demandes de déménagement récupérées : {} demandes", moves.size());
        return moves;
    }

    public void initiateDriverAssignment(String moveId) {
        log.info("Initiating driver assignment for moveId: {}", moveId);
        MoveRequest moveRequest = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> {
                    log.error("Move request not found for moveId: {}", moveId);
                    return new IllegalArgumentException("Move request not found: " + moveId);
                });

        LocalDateTime plannedDateTime = moveRequest.getPlannedDateTime(); // Assumes MoveRequest has this method
        if (plannedDateTime == null) { // Covers null plannedDate or plannedTime
            // Immediate move: start driver assignment now
            log.info("No planned date/time for moveId: {}, treating as immediate move", moveId);
            startDriverAssignment(moveId);
        } else {
            // Planned move: schedule driver assignment 1 hour before planned time
            LocalDateTime assignmentTime = plannedDateTime.minusHours(1);
            LocalDateTime now = LocalDateTime.now();
            if (assignmentTime.isBefore(now)) {
                log.info("Planned time is in the past for moveId: {}, starting assignment immediately", moveId);
                startDriverAssignment(moveId);
            } else {
                log.info("Scheduling driver assignment for moveId: {} at {}", moveId, assignmentTime);
                taskScheduler.schedule(() -> startDriverAssignment(moveId),
                        Date.from(assignmentTime.atZone(ZoneId.systemDefault()).toInstant()));
            }
        }
    }

    @Override
    public List<DriverMissionSummaryDTO> getDriverMissionSummaries(String driverId) {
        log.info("Fetching mission summaries for driverId: {} by user: {}", driverId);

        // Validate driver existence and permissions

        // Query missions where the driver was involved
        Query query = new Query();
        query.addCriteria(
                new Criteria().orOperator(
                        Criteria.where("driver.id").is(driverId),
                        Criteria.where("candidateDrivers").in(driverId)
                )
        );
        List<MoveRequest> missions = mongoTemplate.find(query, MoveRequest.class);

        // Transform missions into DriverMissionSummaryDTO
        List<DriverMissionSummaryDTO> summaries = missions.stream().map(moveRequest -> {
            DriverMissionSummaryDTO summary = new DriverMissionSummaryDTO();
            summary.setMoveId(moveRequest.getMoveId());

            // Calculate duration
            int durationMinutes = googleMapsService.getDurationInMinutes(
                    moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
            String duration = String.format("%dh%02d", durationMinutes / 60, durationMinutes % 60);
            summary.setDuration(duration);

            // Set distance
            double distanceKm = googleMapsService.getDistance(
                    moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
            summary.setDistanceKm(distanceKm);

            // Set source address
            summary.setSourceAddress(moveRequest.getSourceAddress());

            // Calculate product count
            int productCount = moveRequest.getItems().stream()
                    .mapToInt(ItemQuantity::getQuantity)
                    .sum();
            summary.setProductCount(productCount);

            // Set amount (post-commission cost for drivers)
            summary.setAmount(moveRequest.getPostCommissionCost());

            // Set status
            if (moveRequest.getAssignmentStatus() == null) {
                summary.setStatus("NO_STATUS"); // Default status
            }else {
                summary.setStatus(moveRequest.getAssignmentStatus());
            }

            return summary;
        }).filter(m -> m.getStatus().equals("DRIVER_FREE")).collect(Collectors.toList());

        log.info("Mission summaries retrieved successfully for driverId: {}, {} missions", driverId, summaries.size());
        return summaries;
    }

    @Override
    public void updateMoveLocation(String moveId, GeoJsonPoint location) {
        log.info("Mise à jour de l'emplacement pour moveId : {}", moveId);
        MoveRequest moveRequest = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> {
                    logResourceNotFound(moveId);
                    return new RuntimeException("Move request not found: " + moveId);
                });
        moveRequest.setCurrentLocation(location);
        MissionHistory event = new MissionHistory(moveId, "LOCATION_UPDATED", LocalDateTime.now(), "Location updated", "SYSTEM");
        moveRequest.getHistoryEvents().add(event);
        moveRequestRepository.save(moveRequest);
        messagingTemplate.convertAndSend("/topic/mission/" + moveId + "/location",
                new LocationUpdateRequest(location.getY(), location.getX()));
        log.info("Emplacement mis à jour avec succès pour moveId : {}", moveId);
    }

    @Override
    public void addPhotosToMove(String moveId, List<String> photoLinks) {
        log.info("Ajout de photos pour moveId : {}", moveId);
        MoveRequest moveRequest = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> {
                    logResourceNotFound(moveId);
                    return new RuntimeException("Move request not found: " + moveId);
                });

        if (moveRequest.getPhotoLinks() == null) {
            moveRequest.setPhotoLinks(new ArrayList<>());
        }
        moveRequest.getPhotoLinks().addAll(photoLinks);
        moveRequest.setPhotoConfirmationToken(UUID.randomUUID().toString());
        moveRequest.setPhotoConfirmationTokenExpiry(LocalDateTime.now().plusHours(24));
        moveRequest.setPhotosConfirmed(false);
        MissionHistory event = new MissionHistory(moveId, "PHOTOS_ADDED", LocalDateTime.now(), "Photos uploaded", "SYSTEM");
        moveRequest.getHistoryEvents().add(event);
        moveRequestRepository.save(moveRequest);
//        Notification notification = new Notification();
//        notification.setTitle("Mise à jour de votre commande");
//        notification.setBody("Un changement a été apporté à votre réservation. Vérifiez les détails (photos ajoutées).");
//        notification.setTimestamp(LocalDateTime.now());
//        notification.setRead(false);
//        notification.setNotificationType("ORDER_UPDATE");
//        notification.setRelatedMoveId(moveId);
//        notification.setUserId(moveRequest.getClient() != null ? moveRequest.getClient().getId() : null);
//        notification.setStatus("SENT");
//
//        notificationService.sendAndSaveNotification(notification, moveRequest);
        log.info("Photos ajoutées avec succès pour moveId : {}", moveId);
    }

    @Override
    public void confirmPhotos(String token) {
        log.info("Tentative de confirmation des photos avec le jeton : {}", token);
        MoveRequest moveRequest = moveRequestRepository.findByPhotoConfirmationToken(token)
                .orElseThrow(() -> {
                    log.warn("Échec de la confirmation des photos : Jeton invalide");
                    return new IllegalArgumentException("Invalid token");
                });
        if (moveRequest.getPhotoConfirmationTokenExpiry().isBefore(LocalDateTime.now())) {
            log.warn("Échec de la confirmation des photos : Jeton expiré");
            throw new IllegalArgumentException("Token expired");
        }
        moveRequest.setPhotosConfirmed(true);
        moveRequest.setPhotoConfirmationToken(null);
        moveRequest.setPhotoConfirmationTokenExpiry(null);
        MissionHistory event = new MissionHistory(moveRequest.getMoveId(), "PHOTOS_CONFIRMED", LocalDateTime.now(), "Photos confirmed", "SYSTEM");
        moveRequest.getHistoryEvents().add(event);
        moveRequestRepository.save(moveRequest);
        log.info("Photos confirmées avec succès pour moveId : {}", moveRequest.getMoveId());
    }

    @Override
    public void confirmMoveRequest(String token) {
        log.info("Tentative de confirmation de la demande de déménagement avec le jeton : {}", token);
        MoveRequest moveRequest = moveRequestRepository.findByConfirmationToken(token)
                .orElseThrow(() -> {
                    log.warn("Échec de la confirmation de la demande : Jeton invalide");
                    return new IllegalArgumentException("Invalid token");
                });
        if (moveRequest.getConfirmationTokenExpiry().isBefore(LocalDateTime.now())) {
            log.warn("Échec de la confirmation de la demande : Jeton expiré");
            throw new IllegalArgumentException("Token expired");
        }
        moveRequest.setStatus(QuotationStatus.APPROVED);
        moveRequest.setConfirmationToken(null);
        moveRequest.setConfirmationTokenExpiry(null);
        MissionHistory event = new MissionHistory(moveRequest.getMoveId(), "STATUS_CHANGED", LocalDateTime.now(), "Quote status changed to APPROVED", "SYSTEM");
        moveRequest.getHistoryEvents().add(event);
        moveRequestRepository.save(moveRequest);
        log.info("Demande de déménagement confirmée avec succès pour moveId : {}", moveRequest.getMoveId());
    }
    // In MoveServiceImpl.java, add the following method:

// In MoveServiceImpl.java, replace the assignNextPendingToDriver method with:

    public void assignNextPendingToDriver(String driverId) {

        log.info("Offering next pending mission to driver: {}", driverId);

        Query pendingQuery = new Query();
        pendingQuery.addCriteria(Criteria.where("assignmentStatus")
                .in(Arrays.asList("NO_DRIVERS_AVAILABLE", "NO_DRIVERS_IN_RANGE", "COMPLETED"))
                .and("paymentStatus").is("paid"));
        pendingQuery.with(org.springframework.data.domain.Sort.by(org.springframework.data.domain.Sort.Direction.ASC, "confirmationTokenExpiry"));
        pendingQuery.limit(1);
        MoveRequest nextPending = mongoTemplate.findOne(pendingQuery, MoveRequest.class);
        if (nextPending != null) {
            Driver driver = driverRepository.findById(driverId).orElse(null);
            if (driver == null) {
                log.warn("Driver not found: {}", driverId);
                return;
            }

            // Set mission to WAITING_FOR_DRIVER and assign driver as candidate
            nextPending.setAssignmentStatus("WAITING_FOR_DRIVER");
            nextPending.setCandidateDrivers(List.of(driverId));
            nextPending.setCurrentDriverIndex(0);

            // Record assignment offer in history
            MissionHistory event = new MissionHistory(
                    nextPending.getMoveId(),
                    "MISSION_OFFERED_TO_DRIVER",
                    LocalDateTime.now(),
                    String.format("Offer sent to driver %s automatically after previous completion", driverId),
                    "SYSTEM"
            );
            nextPending.getHistoryEvents().add(event);

            log.info("Next pending mission details: {}", nextPending);

            User clientDetails = userRepository.findByEmail(nextPending.getClientEmail()).orElse(null);

            // Create MissionNotification with mission details
            double distanceInKm = googleMapsService.getDistance(nextPending.getSourceAddress(), nextPending.getDestinationAddress(), nextPending.getClientEmail());
            int durationInMinutes = googleMapsService.getDurationInMinutes(nextPending.getSourceAddress(), nextPending.getDestinationAddress(), nextPending.getClientEmail());
            int totalItems = nextPending.getItems().stream().mapToInt(ItemQuantity::getQuantity).sum();

            DecimalFormat decimalFormat = new DecimalFormat("#0.00");
            String formattedPostCommissionCost = decimalFormat.format(nextPending.getPostCommissionCost());
            String formattedDistanceInKm = decimalFormat.format(distanceInKm);
            String formattedDurationInMinutes = String.valueOf(durationInMinutes);
            String formattedTotalItems = String.valueOf(totalItems);

            assert clientDetails != null;
            MissionNotification missionNotification = new MissionNotification(
                    nextPending.getMoveId(),
                    nextPending.getSourceAddress(),
                    nextPending.getDestinationAddress(),
                    formattedPostCommissionCost,
                    formattedDurationInMinutes,
                    formattedTotalItems,
                    formattedDistanceInKm,
                    60, // Timeout for driver response
                    "OFFERED",
                    clientDetails.getLastName() + ' ' + clientDetails.getFirstName(),
                    clientDetails.getPhoneNumber(),
                    nextPending.getPlannedDate(),
                    nextPending.getPlannedTime(),
                    nextPending.getItems()
            );

            String body;
            try {
                body = objectMapper.writeValueAsString(missionNotification);
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize MissionNotification to JSON", e);
                return;
            }

            // Create and send notification
            Notification notification = new Notification();
            notification.setTitle("New Mission Offer");
            notification.setBody(body);
            notification.setTimestamp(LocalDateTime.now());
            notification.setRead(false);
            notification.setNotificationType("MISSION_OFFER");
            notification.setRelatedMoveId(nextPending.getMoveId());
            notification.setUserId(driverId);
            notification.setStatus("SENT");

            notificationService.sendAndSaveNotification(notification, missionNotification);
            log.info("Mission offer notification sent to driver {} for moveId: {}", driverId, nextPending.getMoveId());

            // Schedule timeout for driver response
            ScheduledFuture<?> future = taskScheduler.schedule(() -> checkAssignmentTimeout(nextPending.getMoveId()),
                    new Date(System.currentTimeMillis() + 60 * 1000)); // 60 seconds timeout
            assignmentTimeouts.put(nextPending.getMoveId(), future);

            if (nextPending.getEstimatedTotalMinutes() == null) {
                int stairTimeTotalSeconds = 0;
                int totalFloors = nextPending.getSourceFloors() + nextPending.getDestinationFloors();

                for (ItemQuantity iq : nextPending.getItems()) {
                    int itemStairTimeSeconds;
                    if (totalFloors > 0) {
                        itemStairTimeSeconds = iq.getStairTime() * totalFloors;
                    } else {
                        itemStairTimeSeconds = iq.getStairTime();
                    }
                    stairTimeTotalSeconds += itemStairTimeSeconds;
                }

                int riskMarginMinutes = 15;
                int estimatedTotalMinutes = durationInMinutes + (int) Math.ceil(stairTimeTotalSeconds / 60.0) + riskMarginMinutes;
                nextPending.setEstimatedTotalMinutes(estimatedTotalMinutes);
                log.info("Estimated total minutes calculated and set for pending moveId: {} to {}", nextPending.getMoveId(), estimatedTotalMinutes);
            }

            // Save the updated move request
            moveRequestRepository.save(nextPending);

            Notification clientNotification = new Notification();
            clientNotification.setTitle("Mission Offer Sent to Driver");
            clientNotification.setNotificationType("MISSION_STATUS_UPDATE");
            clientNotification.setTimestamp(LocalDateTime.now());
            clientNotification.setRead(false);
            clientNotification.setRelatedMoveId(nextPending.getMoveId());
            clientNotification.setUserId(clientDetails.getId());
            clientNotification.setStatus("SENT");
            notificationService.sendAndSaveNotification(clientNotification, nextPending);
        } else {
            log.info("No pending missions to offer to driver: {}", driverId);
        }
    }
    @Override
    public void completeMove(String moveId) {
        log.info("Tentative de marquage du déménagement comme terminé pour moveId : {}", moveId);
        MoveRequest moveRequest = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> {
                    logResourceNotFound(moveId);
                    return new RuntimeException("Move request not found: " + moveId);
                });
        moveRequest.setStatus(QuotationStatus.COMPLETED);
        moveRequest.setAssignmentStatus("COMPLETED");
        MissionHistory event = new MissionHistory(moveId, "STATUS_CHANGED", LocalDateTime.now(), "Quote status changed to COMPLETED", "SYSTEM");
        moveRequest.getHistoryEvents().add(event);
        moveRequestRepository.save(moveRequest);
        messagingTemplate.convertAndSend("/topic/mission/" + moveId + "/updates", moveRequest);

        // Notify client about completion
        Notification notification = new Notification();
        notification.setTitle("Chargement terminé, en route vers la destination");
        notification.setBody("Vos biens sont en sécurité dans le camion et en direction de votre destination.");
        notification.setTimestamp(LocalDateTime.now());
        notification.setRead(false);
        notification.setNotificationType("LOADING_COMPLETE");
        notification.setRelatedMoveId(moveId);
        notification.setUserId(moveRequest.getClient() != null ? moveRequest.getClient().getId() : null);
        notification.setStatus("SENT");

        notificationService.sendAndSaveNotification(notification, moveRequest);
        log.info("Déménagement marqué comme terminé pour moveId : {}", moveId);

        // Retry assignment for moves that previously had no available drivers (now that this driver is free)
        Query pendingQuery = new Query();
        pendingQuery.addCriteria(Criteria.where("assignmentStatus")
                .in(Arrays.asList("NO_DRIVERS_AVAILABLE", "NO_DRIVERS_IN_RANGE")));
        List<MoveRequest> pendingMoves = mongoTemplate.find(pendingQuery, MoveRequest.class);
        for (MoveRequest pending : pendingMoves) {
            startDriverAssignment(pending.getMoveId());
        }
    }

    @Override
    public List<String> viewMovePhotos(String moveId, User currentUser) {
        log.info("Tentative de visualisation des photos pour moveId : {} par l'utilisateur : {}", moveId, currentUser.getEmail());
        MoveRequest moveRequest = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> {
                    logResourceNotFound(moveId);
                    return new RuntimeException("Move request not found: " + moveId);
                });
        Role userRole = currentUser.getRole();
        if (userRole == Role.CLIENT && !moveRequest.getClientEmail().equalsIgnoreCase(currentUser.getEmail())) {
            log.warn("Échec de la visualisation des photos : Accès refusé, utilisateur non client pour moveId : {}", moveId);
            throw new RuntimeException("Access denied: This is not your move");
        } else if (userRole == Role.SUB_ADMIN && (moveRequest.getDriver() == null ||
                !((Driver) moveRequest.getDriver()).getCreatedBySubAdminId().getId().equals(currentUser.getId()))) {
            log.warn("Échec de la visualisation des photos : Accès refusé, chauffeur non créé par ce sous-admin pour moveId : {}", moveId);
            throw new RuntimeException("Access denied: This driver does not belong to you");
        } else if (userRole != Role.CLIENT && userRole != Role.SUB_ADMIN) {
            log.warn("Échec de la visualisation des photos : Accès refusé, rôle invalide pour moveId : {}", moveId);
            throw new RuntimeException("Access denied: Invalid role");
        }
        List<String> photoLinks = moveRequest.getPhotoLinks() != null ? moveRequest.getPhotoLinks() : List.of();
        log.info("Photos récupérées avec succès pour moveId : {}, {} photos", moveId, photoLinks.size());
        return photoLinks;
    }

    private List<Item> validateAndFetchItems(List<ItemQuantity> items) {
        log.info("Validation et récupération des objets pour {} éléments", items.size());

        // Step 1: Extract and log item labels, check for null/empty
        List<String> itemLabels = items.stream()
                .map(itemQuantity -> {
                    String label = itemQuantity.getItemLabel();
                    if (label == null || label.trim().isEmpty()) {
                        log.warn("Found null or empty item label in input");
                    }
                    return label;
                })
                .collect(Collectors.toList());
        log.info("Input item labels: {}", itemLabels);

        // Step 2: Check for duplicates
        Set<String> uniqueInputLabels = new HashSet<>(itemLabels);
        if (uniqueInputLabels.size() != itemLabels.size()) {
            Map<String, Long> labelCounts = itemLabels.stream()
                    .filter(label -> label != null)
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            List<String> duplicates = labelCounts.entrySet().stream()
                    .filter(entry -> entry.getValue() > 1)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            log.warn("Duplicate item labels detected: {}", duplicates);
        }

        // Step 3: Fetch items from repository
        List<Item> fetchedItems = itemRepository.findByLabelIn(itemLabels);

        // Step 4: Validate and identify missing labels
        if (fetchedItems.size() != itemLabels.size()) {
            Set<String> fetchedLabels = fetchedItems.stream()
                    .map(Item::getLabel)
                    .collect(Collectors.toSet());
            Set<String> missingLabels = new HashSet<>(uniqueInputLabels);
            missingLabels.removeAll(fetchedLabels);

            String errorMessage = "Invalid item labels.";
            if (!missingLabels.isEmpty()) {
                errorMessage += " Missing labels: " + missingLabels;
            }
            if (uniqueInputLabels.size() != itemLabels.size()) {
                errorMessage += " Duplicates detected.";
            }
            log.warn("Échec de la validation : {}", errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        log.info("Objets validés et récupérés avec succès");
        return fetchedItems;
    }

    private int determineTruckSize(List<Item> items) {
        int truckSize = items.stream().mapToInt(Item::getMinTruckSize).max().orElse(6);
        log.info("Taille de camion déterminée : {} m³", truckSize);
        return truckSize;
    }

    private double predictPrice(double distanceKm, int truckSize) {
        log.info("Prédiction du prix pour une distance de {} km et une taille de camion de {} m³", distanceKm, truckSize);
        if (distanceKm < 20) {
            double price = truckSize == 12 ? 107.91 : 129.71;
            log.info("Prix prédit : {} €", price);
            return price;
        } else {
            double V = truckSize == 12 ? 58.63 : 80;
            double price = 29 + (2.6 * distanceKm) + V;
            log.info("Prix prédit :  {} €", price);
            return price;
        }
    }

    private double calculateFloorCost(int depFloors, boolean depElevator, int arrFloors, boolean arrElevator) {
        log.info("Calcul du coût des étages : départ={} étages, ascenseur={}, arrivée={} étages, ascenseur={}",
                depFloors, depElevator, arrFloors, arrElevator);
        double depCost = depElevator ? 2.5 * depFloors : 5 * depFloors;
        double arrCost = arrElevator ? 2.5 * arrFloors : 5 * arrFloors;
        double totalCost = depCost + arrCost;
        log.info("Coût des étages calculé : {} €", totalCost);
        return totalCost;
    }

    private void logResourceNotFound(String identifier) {
        log.warn("Demande de déménagement non trouvée avec ID : {}", identifier);
    }

    public Map<String, List<MissionHistory>> getDriverHistoryForMove(String moveId) {
        MoveRequest moveRequest = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> new RuntimeException("Move not found: " + moveId));
        Map<String, List<MissionHistory>> driverHistoryMap = new HashMap<>();
        for (MissionHistory event : moveRequest.getHistoryEvents()) {
            String driverId = extractDriverIdFromDetails(event.details());
            if (driverId != null) {
                driverHistoryMap.computeIfAbsent(driverId, k -> new ArrayList<>()).add(event);
            }
        }
        return driverHistoryMap;
    }

    private String extractDriverIdFromDetails(String details) {
        if (details != null && details.contains("driver ")) {
            String[] parts = details.split("driver ");
            if (parts.length > 1) {
                return parts[1].split(" ")[0];
            }
        }
        return null;
    }

    @Async
    public void sendSpecialRequest(String content, String senderEmail) {
        log.info("Envoi d'une demande spéciale de déménagement de: {}", senderEmail);
        try {
            Context context = new Context();
            context.setVariable("senderEmail", senderEmail);
            context.setVariable("content", content);
            String htmlContent = templateEngine.process("special-move-request", context);
            MimeMessage mimeMessage = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");
            helper.setFrom(senderEmail);
            helper.setTo("ramasquare22@gmail.com");
            helper.setSubject("Demande spéciale de déménagement de " + senderEmail);
            helper.setText(htmlContent, true);
            mailSender.send(mimeMessage);
            log.info("Demande spéciale envoyée avec succès de: {}", senderEmail);
        } catch (MessagingException e) {
            log.error("Échec de l'envoi de la demande spéciale de {}: {}", senderEmail, e.getMessage());
            throw new RuntimeException("Échec de l'envoi de la demande spéciale", e);
        }
    }

    @Override
    public List<MissionHistory> getDriverMissionHistory(String driverId, User currentUser) {
        Driver driver = driverRepository.findById(driverId)
                .orElseThrow(() -> new ResourceNotFoundException("Driver not found: " + driverId));

        if (currentUser.getRole() == Role.SUB_ADMIN) {
            if (!driver.getCreatedBySubAdminId().getId().equals(currentUser.getId())) {
                throw new AccessDeniedException("You do not have permission to view this driver's history.");
            }
        }

        Query query = new Query();
        query.addCriteria(
                new Criteria().orOperator(
                        Criteria.where("historyEvents.triggeredBy").is(driverId),
                        Criteria.where("historyEvents.details").regex("driver " + driverId)
                )
        );
        List<MoveRequest> missions = mongoTemplate.find(query, MoveRequest.class);

        List<MissionHistory> allEvents = new ArrayList<>();
        for (MoveRequest mission : missions) {
            for (MissionHistory event : mission.getHistoryEvents()) {
                if (event.triggeredBy().equals(driverId) ||
                        event.details().contains("driver " + driverId)) {
                    allEvents.add(event);
                }
            }
        }

        allEvents.sort(Comparator.comparing(MissionHistory::timestamp));
        return allEvents;
    }

    @Override
    public List<MoveRequest> getMovesForSubadmin(String subadminId) {
        Query driverQuery = new Query(Criteria.where("createdBySubAdminId.$id").is(new ObjectId(subadminId)));
        List<Driver> drivers = mongoTemplate.find(driverQuery, Driver.class);
        List<String> driverIds = drivers.stream().map(User::getId).collect(Collectors.toList());
        if (driverIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<ObjectId> driverObjectIds = driverIds.stream().map(ObjectId::new).collect(Collectors.toList());
        Query moveQuery = new Query(Criteria.where("driver.$id").in(driverObjectIds));
        return mongoTemplate.find(moveQuery, MoveRequest.class);
    }

    @Override
    public void updateMoveItems(String moveId, List<ItemQuantity> items, User currentUser) {
        log.info("Attempting to update items for moveId: {} by user: {}", moveId, currentUser.getEmail());

        // Fetch the MoveRequest
        MoveRequest moveRequest = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> {
                    log.error("MoveRequest not found for moveId: {}", moveId);
                    return new ResourceNotFoundException("Demande de déménagement non trouvée");
                });

        // Validate user permissions (CLIENT or DRIVER associated with the move)
        if (moveRequest.getClient() == null || moveRequest.getDriver() == null ||
                (!moveRequest.getClient().getId().equals(currentUser.getId()) &&
                        !moveRequest.getDriver().getId().equals(currentUser.getId()))) {
            log.warn("User {} is not authorized to update items for moveId: {}", currentUser.getEmail(), moveId);
            throw new ForbiddenException("Vous n'êtes pas autorisé à modifier les objets de cette mission");
        }

        // Validate items list
        if (items == null || items.isEmpty()) {
            log.warn("Invalid items list provided for moveId: {}", moveId);
            throw new BadRequestException("La liste des objets ne peut pas être nulle ou vide");
        }

        // Update items
        moveRequest.setVerifiedItemsByDriver(items);
        moveRequestRepository.save(moveRequest);

        log.info("Items updated successfully for moveId: {}", moveId);
    }
    @Override
    public MoveDetails getLastMinuteMoveDetails(String moveId) {
        log.info("Fetching last-minute move details for moveId: {}", moveId);

        LastMinuteMove lastMinuteMove = lastMinuteMoveRepository.findById(moveId)
                .orElseThrow(() -> {
                    log.error("Last-minute move not found for moveId: {}", moveId);
                    return new ResourceNotFoundException("Last-minute move not found: " + moveId);
                });

        double distance = googleMapsService.getDistance(
                lastMinuteMove.getSourceAddress(),
                lastMinuteMove.getDestinationAddress(),
                lastMinuteMove.getClientEmail());
        int durationMinutes = googleMapsService.getDurationInMinutes(
                lastMinuteMove.getSourceAddress(),
                lastMinuteMove.getDestinationAddress(),
                lastMinuteMove.getClientEmail());

        LocalDateTime departureTime = lastMinuteMove.getAssignmentTimestamp() != null
                ? lastMinuteMove.getAssignmentTimestamp()
                : lastMinuteMove.getCreatedAt() != null
                ? lastMinuteMove.getCreatedAt()
                : LocalDateTime.now();
        LocalDate date = departureTime.toLocalDate();
        LocalDateTime arrivalTime = departureTime.plusMinutes(durationMinutes);

        return new MoveDetails(
                distance,
                durationMinutes,
                date,
                departureTime,
                arrivalTime,
                lastMinuteMove.getItems(),
                lastMinuteMove.getMissionStatus() != null
                        ? lastMinuteMove.getMissionStatus().getNameValue()
                        : "PENDING",
                lastMinuteMove.getSourceAddress(),
                lastMinuteMove.getDestinationAddress()
        );
    }

    @Override
    public LastMinuteMove createFromAcceptedMove(String moveId, LastMinuteMoveRequest request) throws IOException, InterruptedException, ApiException, ExecutionException {
        log.info("🚚 Creating LAST_MINUTE move from moveId: {}", moveId);
        MoveRequest originalMove = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> new IllegalArgumentException("Move request not found: " + moveId));

        if (originalMove.getDriver() == null) {
            throw new IllegalStateException("No driver assigned to the original move.");
        }

        LastMinuteMove lastMinuteMove = new LastMinuteMove();
        lastMinuteMove.setSourceAddress(originalMove.getDestinationAddress());
        lastMinuteMove.setDestinationAddress(request.getDestinationAddress());
        lastMinuteMove.setEstimatedTotalMinutes(originalMove.getEstimatedTotalMinutes());
        lastMinuteMove.setCreatedAt(LocalDateTime.now());
        lastMinuteMove.setDriver(originalMove.getDriver());
        lastMinuteMove.setMode(QuotationType.LAST_MINUTE);
        List<Itinerary> itineraries = googleMapsService.getItineraries(
                originalMove.getDestinationAddress(),
                request.getDestinationAddress()
        );

        log.info("🗺️ Itineraries fetched: {}", itineraries);
        lastMinuteMove.setItineraries(itineraries);
        LastMinuteMove savedMove = lastMinuteMoveRepository.save(lastMinuteMove);
        log.info("✅ LAST_MINUTE move created: {}", savedMove.getId());


        return savedMove;
    }
    @Override
    public LastMinuteMove calculateLastMinuteQuote(String lastMinuteMoveId, QuoteCalculationRequest request, String email) {
        log.info("📦 Calculating LAST_MINUTE quote for moveId: {}", lastMinuteMoveId);

        // Rate limit check
        Bucket bucket = getUserBucket(email);
        if (!bucket.tryConsume(1)) {
            log.warn("Rate limit exceeded  for user: {}. Limit: 20 requests per 24 hours.", email);
            throw new BadRequestException("Vous avez dépassé la limite de 20 demandes de devis par 24 heures. Veuillez réessayer plus tard.");
        }

        if (request.items() == null || request.items().isEmpty()) {
            throw new IllegalArgumentException("Items are required.");
        }

        LastMinuteMove lastMinuteMove = lastMinuteMoveRepository.findById(lastMinuteMoveId)
                .orElseThrow(() -> new IllegalArgumentException("LastMinuteMove not found: " + lastMinuteMoveId));

        if (lastMinuteMove.getDestinationAddress() == null || lastMinuteMove.getDestinationAddress().isBlank()) {
            throw new IllegalArgumentException("Destination address is missing.");
        }
        if (request.stopover() == null || request.stopover().isBlank()) {
            throw new IllegalArgumentException("Stopover address is missing.");
        }
        if (request.destinationStopover() == null || request.destinationStopover().isBlank()) {
            throw new IllegalArgumentException("Destination stopover address is missing.");
        }

        log.info("Validating stopover '{}' and destination stopover '{}' against selected itineraries",
                request.stopover(), request.destinationStopover());


        // TODO : uncomment when needed

//        boolean isStopoverValid = lastMinuteMove.getSelectedItineraries().stream()
//                .anyMatch(itinerary -> itinerary.getCities().contains(request.stopover()));
//        boolean isDestinationStopoverValid = lastMinuteMove.getSelectedItineraries().stream()
//                .anyMatch(itinerary -> itinerary.getCities().contains(request.destinationStopover()));

//        if (!isStopoverValid) {
//            throw new IllegalArgumentException(
//                    "⛔ Stopover '" + request.stopover() + "' is not part of the planned route."
//            );
//        }
//        if (!isDestinationStopoverValid) {
//            throw new IllegalArgumentException(
//                    "⛔ Destination stopover '" + request.destinationStopover() + "' is not part of the planned route."
//            );
//        }
//        String clientAddress = request.clientAddressPoint();
//        if (!googleMapsService.isAddressInCity(clientAddress, request.stopover(), email)) {
//            throw new IllegalArgumentException(
//                    "⛔ Client location is not inside the stopover city: " + request.stopover()
//            );
//        }
//
//        String clientAddressDestination = request.clientDestinationPoint();
//        if (!googleMapsService.isAddressInCity(clientAddressDestination, request.destinationStopover(), email)) {
//            throw new IllegalArgumentException(
//                    "⛔ Client location is not inside the stopover destination city: " + request.destinationStopover()
//            );
//        }

        // Distance check
        double distanceKm = googleMapsService.getDistance(
                request.stopover(),
                request.destinationStopover(),
                email
        );
        log.info("Distance from destination to stopover: {} km", distanceKm);

        // Validate and fetch items
        List<Item> items = validateAndFetchItems(request.items());
        int totalFloors = lastMinuteMove.getSourceFloors() + request.destinationFloors();
        double totalVolume = 0.0;
        int maxMinTruckSize = 0;
        int totalItems = 0;
        List<ItemQuantity> updatedItems = new ArrayList<>();

        for (ItemQuantity iq : request.items()) {
            String key = iq.getItemLabel();
            Item item = items.stream().filter(i -> i.getLabel().equals(key)).findFirst().orElseThrow();

            totalVolume += Double.parseDouble(item.getVolume()) * iq.getQuantity();
            if (iq.getQuantity() > 0) maxMinTruckSize = Math.max(maxMinTruckSize, item.getMinTruckSize());

            // Compute stairTime
            double stairTime;
            if (item.getStaiTime() != null && !item.getStaiTime().isEmpty()) {
                stairTime = Double.parseDouble(item.getStaiTime()) * iq.getQuantity();
                iq.setStairTime((int) stairTime);
            }

            totalItems += iq.getQuantity();
            updatedItems.add(iq);
        }

        // Truck calculation
        class Truck { final int volume; final double price; Truck(int volume, double price) { this.volume = volume; this.price = price; } }
        List<Truck> truckTypes = Arrays.asList(new Truck(12, 107.91), new Truck(20, 129.7));
        int minTruckVolume = truckTypes.stream().mapToInt(t -> t.volume).min().orElse(0);
        int maxTruckVolume = truckTypes.stream().mapToInt(t -> t.volume).max().orElse(0);

        if (maxMinTruckSize > maxTruckVolume || maxMinTruckSize < minTruckVolume) {
            throw new IllegalArgumentException("Selected items require a truck size incompatible with available trucks.");
        }

        // Base price calculation
        double basePrice;
        if (totalVolume == 0) basePrice = 0;
        else if (maxMinTruckSize > 12) basePrice = Math.ceil(totalVolume / 20.0) * 129.7;
        else {
            double minCost = Double.MAX_VALUE;
            int maxLarge = (int) Math.ceil(totalVolume / 12.0);
            for (int numLarge = 0; numLarge <= maxLarge; numLarge++) {
                double remainingVolume = Math.max(0, totalVolume - numLarge * 20.0);
                int numSmall = (int) Math.ceil(remainingVolume / 12.0);
                double cost = numLarge * 129.7 + numSmall * 107.91;
                if (cost < minCost) minCost = cost;
            }
            basePrice = minCost;
        }

        // Handling cost
        double totalStairTime = updatedItems.stream().mapToDouble(ItemQuantity::getStairTime).sum();
        double handlingMinutes = totalStairTime / 60.0;
        double handlingCost;

        if (totalFloors == 0 && totalItems < 15) {
            handlingCost = 0;
        } else if (totalFloors == 0) {
            List<double[]> bracketsZero = Arrays.asList(
                    new double[]{15, 1}, new double[]{20, 1.5}, new double[]{25, 2}, new double[]{30, 2.5},
                    new double[]{35, 4.5}, new double[]{40, 6}, new double[]{45, 7}, new double[]{50, 10},
                    new double[]{55, 12}, new double[]{60, 20}, new double[]{65, 26}, new double[]{70, 40}
            );
            double rate = 49.8;
            for (double[] bracket : bracketsZero) if (totalItems <= bracket[0]) { rate = bracket[1]; break; }
            handlingCost = rate * handlingMinutes;
        } else {
            List<double[]> bracketsFirst = Arrays.asList(
                    new double[]{10, 3}, new double[]{20, 4.375}, new double[]{30, 11.5}, new double[]{40, 15},
                    new double[]{50, 20}, new double[]{60, 34.54}, new double[]{70, 64.74}
            );
            List<double[]> bracketsAdditional = Arrays.asList(
                    new double[]{10, 3}, new double[]{20, 5}, new double[]{30, 10}, new double[]{40, 14.4},
                    new double[]{50, 21}, new double[]{60, 34.5}, new double[]{70, 124.5}
            );
            double firstRate = 49.8, additionalRate = 49.8;
            for (double[] bracket : bracketsFirst) if (totalItems <= bracket[0]) { firstRate = bracket[1]; break; }
            for (double[] bracket : bracketsAdditional) if (totalItems <= bracket[0]) { additionalRate = bracket[1]; break; }
            handlingCost = firstRate * handlingMinutes + additionalRate * handlingMinutes * (totalFloors - 1);
        }

        double urgencyMultiplier = 1.0;
        double totalPrice = (basePrice + handlingCost) * urgencyMultiplier;
        if (distanceKm > 20) totalPrice += 2.6 * distanceKm;

        // Apply 50% discount
        double discountedPrice = totalPrice * 0.5;

        Admin admin = (Admin) userRepository.findByRole(Role.ADMIN).getFirst();
        double postCommissionCost = discountedPrice * (1 - admin.getCommissionRate());
        Optional<User> clientOpt = userRepository.findByEmail(email);
        if (clientOpt.isPresent()) {
            lastMinuteMove.setClient(clientOpt.get());
            lastMinuteMove.setClientEmail(clientOpt.get().getEmail());
        } else {
            throw new IllegalArgumentException("Authenticated user not found in the system.");
        }

        // Update LastMinuteMove
        lastMinuteMove.setEstimatedTotalMinutes((int) Math.ceil(totalStairTime + totalFloors * 10));
        lastMinuteMove.setPreCommissionCost(totalPrice);
        lastMinuteMove.setPreCommissionCostAfterDiscount(discountedPrice);
        lastMinuteMove.setPostCommissionCost(postCommissionCost);
        lastMinuteMove.setItems(updatedItems);
        lastMinuteMove.setMode(QuotationType.LAST_MINUTE);
        lastMinuteMove.setStopover(request.stopover());
        lastMinuteMove.setDestinationStopover(request.destinationStopover());
        lastMinuteMove.setClientAddressPoint(request.clientAddressPoint());
        lastMinuteMove.setClientDestinationPoint(request.clientDestinationPoint());

        lastMinuteMoveRepository.save(lastMinuteMove);

        log.info("✅ LAST_MINUTE quote calculated successfully for moveId: {}", lastMinuteMoveId);

        return lastMinuteMove;
    }

    @Override
    public void assignDriverToMission(String moveId, String driverId, User subadmin) {
        log.info("Assigning driver {} to mission {} by subadmin {}", driverId, moveId, subadmin.getId());

        // Validate move request
        MoveRequest moveRequest = moveRequestRepository.findById(moveId)
                .orElseThrow(() -> {
                    log.error("Move request not found for moveId : {}", moveId);
                    return new ResourceNotFoundException("Demande de déménagement non trouvée: " + moveId);
                });

        // Validate mission status
        if (!"PENDING".equals(moveRequest.getStatus().name()) && !"APPROVED".equals(moveRequest.getStatus().name())) {
            log.warn("Mission {} is not in a valid state for assignment. Current status: {}", moveId, moveRequest.getStatus());
            throw new BadRequestException("La mission n'est pas dans un état permettant l'assignation (doit être PENDING ou APPROVED)");
        }

        // Validate driver
        Driver driver = driverRepository.findById(driverId)
                .orElseThrow(() -> {
                    log.error("Driver not found for driverId: {}", driverId);
                    return new ResourceNotFoundException("Chauffeur non trouvé: " + driverId);
                });

        // Check if driver is managed by the subadmin
        if (driver.getCreatedBySubAdminId() == null || !driver.getCreatedBySubAdminId().getId().equals(subadmin.getId())) {
            log.warn("Driver {} is not managed by subadmin {}", driverId, subadmin.getId());
            throw new ForbiddenException("Ce chauffeur n'est pas géré par ce sous-administrateur");
        }

        // Check if driver is online
        DriverLocation driverLocation = onlineDrivers.get(driverId);
        if (driverLocation == null) {
            log.warn("Driver {} is not online", driverId);
            throw new BadRequestException("Le chauffeur n'est pas en ligne");
        }

        // Check if driver is busy
        List<String> busyDriverIds = getBusyDriverIds();
        if (busyDriverIds.contains(driverId)) {
            log.warn("Driver {} is already assigned to another mission", driverId);
            throw new BadRequestException("Le chauffeur est déjà assigné à une autre mission");
        }

        // Set mission to WAITING_FOR_DRIVER and assign driver as candidate
        moveRequest.setAssignmentStatus("WAITING_FOR_DRIVER");
        moveRequest.setCandidateDrivers(List.of(driverId));
        moveRequest.setCurrentDriverIndex(0);

        // Record assignment offer in history
        MissionHistory event = new MissionHistory(
                moveId,
                "MISSION_OFFERED_TO_DRIVER",
                LocalDateTime.now(),
                String.format("Offer sent to driver %s by subadmin %s", driverId, subadmin.getId()),
                subadmin.getId()
        );
        moveRequest.getHistoryEvents().add(event);

        // Create MissionNotification with mission details
        double distanceInKm = googleMapsService.getDistance(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
        int durationInMinutes = googleMapsService.getDurationInMinutes(moveRequest.getSourceAddress(), moveRequest.getDestinationAddress(), moveRequest.getClientEmail());
        int totalItems = moveRequest.getItems().stream().mapToInt(ItemQuantity::getQuantity).sum();

        DecimalFormat decimalFormat = new DecimalFormat("#0.00");
        String formattedPostCommissionCost = decimalFormat.format(moveRequest.getPostCommissionCost());
        String formattedDistanceInKm = decimalFormat.format(distanceInKm);
        String formattedDurationInMinutes = String.valueOf(durationInMinutes);
        String formattedTotalItems = String.valueOf(totalItems);

        MissionNotification missionNotification = new MissionNotification(
                moveRequest.getMoveId(),
                moveRequest.getSourceAddress(),
                moveRequest.getDestinationAddress(),
                formattedPostCommissionCost,
                formattedDurationInMinutes,
                formattedTotalItems,
                formattedDistanceInKm,
                60, // Timeout for driver response
                "OFFERED",
                moveRequest.getClient().getLastName() + ' ' + moveRequest.getClient().getFirstName(),
                moveRequest.getClient().getPhoneNumber(),
                moveRequest.getPlannedDate(),
                moveRequest.getPlannedTime(),
                moveRequest.getItems()
        );

        String body;
        try {
            body = objectMapper.writeValueAsString(missionNotification);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize MissionNotification to JSON", e);
            throw new RuntimeException("Failed to create notification", e);
        }

        // Create and send notification
        Notification notification = new Notification();
        notification.setTitle("New Mission Offer");
        notification.setBody(body);
        notification.setTimestamp(LocalDateTime.now());
        notification.setRead(false);
        notification.setNotificationType("MISSION_OFFER");
        notification.setRelatedMoveId(moveId);
        notification.setUserId(driverId);
        notification.setStatus("SENT");

        notificationService.sendAndSaveNotification(notification, missionNotification);
        log.info("Mission offer  notification sent to driver {} for moveId: {} ", driverId, moveId);

        // Schedule timeout for driver response
        ScheduledFuture<?> future = taskScheduler.schedule(() -> checkAssignmentTimeout(moveId),
                new Date(System.currentTimeMillis() + 60 * 1000)); // 60 seconds timeout
        assignmentTimeouts.put(moveId, future);

        // Save the updated move request
        moveRequestRepository.save(moveRequest);

        log.info("Driver {} offered mission {} by subadmin {}", driverId, moveId, subadmin.getId());
    }
    @Override
    public List<MoveRequest> getFilteredMissionsForSubadmin(String subadminId) {
        log.info("Fetching filtered missions for subadminId: {}", subadminId);

        // Find drivers managed by the subadmin
        Query driverQuery = new Query(Criteria.where("createdBySubAdminId.$id").is(new ObjectId(subadminId)));
        List<Driver> drivers = mongoTemplate.find(driverQuery, Driver.class);
        List<String> driverIds = drivers.stream().map(Driver::getId).toList();

        // Query missions with missionStatus=ACCEPTED or (status=PENDING and paymentStatus=paid)
        Query moveQuery = new Query();
        moveQuery.addCriteria(
                new Criteria().orOperator(
                        // Missions with ACCEPTED status and assigned to subadmin's drivers
                        new Criteria().andOperator(
                                Criteria.where("driver.$id").in(driverIds.stream().map(ObjectId::new).collect(Collectors.toList())),
                                Criteria.where("missionStatus").is(MissionStatus.ACCEPTED.getNameValue())
                        ),
                        // Missions with PENDING status and paid paymentStatus, regardless of driver
                        new Criteria().andOperator(
                                Criteria.where("status").is("PENDING"),
                                Criteria.where("paymentStatus").is("paid")
                        )
                )
        );

        List<MoveRequest> missions = mongoTemplate.find(moveQuery, MoveRequest.class);
        log.info("Filtered missions retrieved successfully for subadminId: {}, {} missions", subadminId, missions.size());
        return missions;
    }

    // In MoveServiceImpl class
    @Scheduled(fixedRate = 3600000) // Every hour
    public void checkAndSendEmailsForDelayedMoves() {
        log.info("Checking for delayed paid moves without assignment...");
        LocalDateTime now = LocalDateTime.now();

        Query query = new Query();
        query.addCriteria(Criteria.where("paymentStatus").is("paid")
                .and("assignmentStatus").in("NO_DRIVERS_AVAILABLE", "NO_DRIVERS_IN_RANGE")
                .and("adminNotified").is(false));

        List<MoveRequest> delayedMoves = mongoTemplate.find(query, MoveRequest.class);

        for (MoveRequest move : delayedMoves) {
            if (move.getDateOfPayment() != null && move.getDateOfPayment().plusHours(24).isBefore(now)) {
                sendEmailToAdmin(move);
                move.setAdminNotified(true);
                moveRequestRepository.save(move);
                log.info("Marked move {} as admin notified", move.getMoveId());
            }
        }

        log.info("Finished checking delayed moves. Processed: {}", delayedMoves.size());
    }

    private void sendEmailToAdmin(MoveRequest move) {
        try {
            Context context = new Context();
            context.setVariable("moveId", move.getMoveId());
            context.setVariable("clientEmail", move.getClientEmail());
            context.setVariable("paymentDate", move.getDateOfPayment());
            context.setVariable("sourceAddress", move.getSourceAddress());
            context.setVariable("destinationAddress", move.getDestinationAddress());
            context.setVariable("assignmentStatus", move.getAssignmentStatus());

            String htmlContent = templateEngine.process("delayed-move-alert", context);

            MimeMessage mimeMessage = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");
            helper.setFrom("ramasquare22@gmail.com"); // Matches spring.mail.username

            // Fetch all users with SUB_ADMIN role
            List<User> subAdmins = userRepository.findByRole(Role.SUB_ADMIN);
            List<String> recipientEmails = new ArrayList<>();
            recipientEmails.add("mohamed.aichaoui.tic@gmail.com"); // Admin email need chnages
            for (User subAdmin : subAdmins) {
                if (subAdmin.getEmail() != null && !subAdmin.getEmail().isEmpty()) {
                    recipientEmails.add(subAdmin.getEmail());
                }
            }

            // Convert list to array for setTo
            if (recipientEmails.isEmpty()) {
                log.warn("No valid recipient emails found for moveId: {}", move.getMoveId());
                return;
            }
            helper.setTo(recipientEmails.toArray(new String[0]));
            helper.setSubject("Alerte: Déménagement payé en attente depuis plus de 24h - ID: " + move.getMoveId());
            helper.setText(htmlContent, true);

            mailSender.send(mimeMessage);
            log.info("Email sent to admin and sub-admins for delayed move: {}. Recipients: {}", move.getMoveId(), recipientEmails);
        } catch (MessagingException e) {
            log.error("Failed to send delayed move alert email for moveId: {}", move.getMoveId(), e);
        }
    }

}