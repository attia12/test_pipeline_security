package fr.tictak.dema.controller;

import fr.tictak.dema.dto.in.DriverLocationMessage;
import fr.tictak.dema.dto.in.DriverStatusMessage;
import fr.tictak.dema.model.LastMinuteMove;
import fr.tictak.dema.model.MissionStatusUpdateRequest;
import fr.tictak.dema.model.MoveRequest;
import fr.tictak.dema.model.Notification;
import fr.tictak.dema.model.enums.MissionStatus;
import fr.tictak.dema.model.user.Driver;
import fr.tictak.dema.repository.DriverRepository;
import fr.tictak.dema.repository.LastMinuteMoveRepository;
import fr.tictak.dema.repository.MoveRequestRepository;
import fr.tictak.dema.service.implementation.MoveServiceImpl;
import fr.tictak.dema.service.implementation.NotificationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Controller;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Optional;

@Controller
@Tag(name = "Gestion des emplacements des chauffeurs", description = "API WebSocket pour la mise à jour en temps réel des statuts et emplacements des chauffeurs. Développée par Mohamed Aichaoui pour BramaSquare.")
public class DriverLocationController {

    private static final Logger logger = LoggerFactory.getLogger(DriverLocationController.class);

    private final SimpMessagingTemplate messagingTemplate;
    private final DriverRepository driverRepository;
    private final MoveServiceImpl moveService;
    private final MoveRequestRepository moveRequestRepository;
    private final LastMinuteMoveRepository lastMinuteMoveRepository;
    private final NotificationService notificationService;
    private final TaskScheduler taskScheduler;

    public DriverLocationController(SimpMessagingTemplate messagingTemplate,
                                    DriverRepository driverRepository,
                                    MoveServiceImpl moveService,
                                    MoveRequestRepository moveRequestRepository,
                                    LastMinuteMoveRepository lastMinuteMoveRepository,
                                    NotificationService notificationService,
                                    TaskScheduler taskScheduler) {
        this.messagingTemplate = messagingTemplate;
        this.driverRepository = driverRepository;
        this.moveService = moveService;
        this.moveRequestRepository = moveRequestRepository;
        this.lastMinuteMoveRepository = lastMinuteMoveRepository;
        this.notificationService = notificationService;
        this.taskScheduler = taskScheduler;
    }

    @Operation(summary = "Mettre à jour le statut du chauffeur", description = "Met à jour le statut d'un chauffeur via un message WebSocket. Développé par Mohamed Aichaoui pour BramaSquare.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Statut mis à jour avec succès"),
            @ApiResponse(responseCode = "400", description = "Chauffeur non trouvé")
    })
    @MessageMapping("/driver/status")
    public void updateDriverStatus(DriverStatusMessage message) {
        logger.info("Mise à jour du statut reçue – utilisateur={}, statut={}", message.userId(), message.status());
        Driver driver = driverRepository.findById(message.userId())
                .orElseThrow(() -> {
                    logDriverNotFound(message.userId());
                    return new IllegalArgumentException("Chauffeur non trouvé : " + message.userId());
                });

        driver.setStatus(message.status());
        driverRepository.save(driver);

        if ("online".equalsIgnoreCase(message.status())) {
            GeoJsonPoint location = driver.getLocation();
            if (location != null) {
                moveService.updateDriverLocation(message.userId(), location.getY(), location.getX());
            }
            Optional<MoveRequest> activeMission = moveRequestRepository.findByDriverIdAndAssignmentStatus(message.userId(), "ASSIGNED");
            if (activeMission.isPresent()) {
                MoveRequest mission = activeMission.get();
                messagingTemplate.convertAndSend("/topic/driver/" + message.userId() + "/mission", mission);
                logger.info("Détails de la mission active envoyés au chauffeur : {}", message.userId());
            }
        } else {
            moveService.removeOnlineDriver(message.userId());
        }

        GeoJsonPoint location = driver.getLocation();
        if (location != null) {
            String subAdminId = driver.getCreatedBySubAdminId().getId();
            messagingTemplate.convertAndSend(
                    "/topic/subadmin/" + subAdminId + "/drivers",
                    new DriverLocationMessage(message.userId(), location.getY(), location.getX(), message.status())
            );
            logger.info("Message de localisation envoyé au sous-administrateur {} pour le chauffeur {}", subAdminId, message.userId());
        }
    }

    @Operation(summary = "Mettre à jour l'emplacement du chauffeur", description = "Met à jour l'emplacement d'un chauffeur via WebSocket. Développé par Mohamed Aichaoui pour BramaSquare.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Emplacement mis à jour avec succès"),
            @ApiResponse(responseCode = "400", description = "Chauffeur non trouvé")
    })
    @MessageMapping("/driver/location")
    public void updateDriverLocation(DriverLocationMessage message) {
        logger.info("Mise à jour de l'emplacement reçue – utilisateur={}, latitude={}, longitude={}", message.driverId(), message.latitude(), message.longitude());
        Driver driver = driverRepository.findById(message.driverId())
                .orElseThrow(() -> {
                    logDriverNotFound(message.driverId());
                    return new IllegalArgumentException("Chauffeur non trouvé : " + message.driverId());
                });

        driver.setLocation(new GeoJsonPoint(message.longitude(), message.latitude()));
        driverRepository.save(driver);

        moveService.updateDriverLocation(message.driverId(), message.latitude(), message.longitude());

        String subAdminId = driver.getCreatedBySubAdminId().getId();
        messagingTemplate.convertAndSend("/topic/subadmin/" + subAdminId + "/drivers", message);

        Optional<MoveRequest> activeMission = moveRequestRepository.findByDriverIdAndAssignmentStatus(message.driverId(), "ASSIGNED");

        if (activeMission.isPresent()) {
            MoveRequest mission = activeMission.get();
            String moveId = mission.getMoveId();
            logger.info("Mission active ASSIGNED trouvée pour le chauffeur {} – moveId={}", message.driverId(), moveId);

            String missionDestination = "/topic/mission/" + moveId + "/driverLocation";
            logger.debug("Envoi de la mise à jour de la localisation du chauffeur au topic : {}", missionDestination);
            logger.debug("Charge utile de la localisation du chauffeur – latitude={}, longitude={}", message.latitude(), message.longitude());

            messagingTemplate.convertAndSend(missionDestination, message);
            logger.info("Mise à jour de la localisation du chauffeur envoyée avec succès à la mission {} via {}", moveId, missionDestination);
        } else {
            logger.info("Aucune mission active ASSIGNED trouvée pour le chauffeur {}", message.driverId());
        }
    }

    @Operation(summary = "Avancer le statut de la mission", description = "Avance le statut de la mission au suivant dans la séquence définie. Développé par Mohamed Aichaoui pour BramaSquare.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Statut de la mission avancé avec succès"),
            @ApiResponse(responseCode = "400", description = "Mission ou chauffeur non valide")
    })
    @MessageMapping("/mission/status/next")
    public void advanceMissionStatus(MissionStatusUpdateRequest request) {
        String moveId = request.getMoveId();
        String driverId = request.getDriverId();
        logger.info("Demande d'avancement du statut reçue – moveId={}, driverId={}", moveId, driverId);

        // Try MoveRequest first
        Optional<MoveRequest> moveRequestOpt = moveRequestRepository.findById(moveId);
        if (moveRequestOpt.isPresent()) {
            MoveRequest moveRequest = moveRequestOpt.get();

            if (!moveRequest.getDriver().getId().equals(driverId)) {
                logger.warn("Chauffeur non autorisé à mettre à jour cette mission : {}", driverId);
                return;
            }

            MissionStatus currentStatus = moveRequest.getMissionStatus();
            MissionStatus nextStatus = currentStatus.next();

            if (nextStatus == currentStatus) {
                logger.info("La mission est déjà au statut final : {}", currentStatus.getDescription());
                return;
            }

            moveRequest.setMissionStatus(nextStatus);
            moveRequestRepository.save(moveRequest);

//            messagingTemplate.convertAndSend("/topic/mission/" + moveId + "/status", nextStatus.getDescription());
//            logger.info("Statut de la mission diffusé au client : moveId={}, statut={}", moveId, nextStatus.getDescription());

            Notification notification = new Notification();
            notification.setTitle("Mise à jour du statut de la mission");
            notification.setNotificationType("MISSION_STATUS_UPDATE");
            notification.setRelatedMoveId(moveId);
            notification.setUserId(moveRequest.getClient().getId());
            notification.setStatus("SENT");
            notification.setTimestamp(LocalDateTime.now());
            notification.setRead(false);
            try {
                notificationService.sendAndSaveNotification(notification, moveRequest);
                logger.info("Notification de mise à jour du statut de la mission envoyée pour le client : userId={}, moveId={}", moveRequest.getClient().getId(), moveId);
            } catch (Exception e) {
                logger.error("Échec de l'envoi de la notification de mise à jour du statut de la mission : userId={}, moveId={}, erreur={}",
                        moveRequest.getClient().getId(), moveId, e.getMessage(), e);
            }

            if (nextStatus == MissionStatus.COMPLETED) {
                moveRequest.setAssignmentStatus("DRIVER_FREE");
                moveRequestRepository.save(moveRequest);

                try {
                    lastMinuteMoveRepository.deleteByDriverId(request.getDriverId());
                    logger.info("LastMinuteMove records deleted for driver {}", driverId);
                } catch (Exception e) {
                    logger.debug("No LastMinuteMove to delete or deletion failed for driver {}: {}", driverId, e.getMessage());
                    // - continue execution
                }
                logger.info("Statut d'affectation du chauffeur {} défini à DRIVER_FREE après achèvement de la mission", driverId);
                moveService.addOnlineDriver(driverId);
                taskScheduler.schedule(() -> moveService.assignNextPendingToDriver(driverId),
                        Instant.now().plusMillis(2000));
            }
            return;
        }

        Optional<LastMinuteMove> lastMinuteMoveOpt = lastMinuteMoveRepository.findById(moveId);
        if (lastMinuteMoveOpt.isPresent()) {
            LastMinuteMove lastMinuteMove = lastMinuteMoveOpt.get();

            if (lastMinuteMove.getDriver() == null || !lastMinuteMove.getDriver().getId().equals(driverId)) {
                logger.warn("Chauffeur non autorisé à mettre à jour cette mission LAST_MINUTE : {}", driverId);
                return;
            }

            MissionStatus currentStatus = lastMinuteMove.getMissionStatus();
            MissionStatus nextStatus = currentStatus.next();

            if (nextStatus == currentStatus) {
                logger.info("La mission (LAST_MINUTE) est déjà au statut final : {}", currentStatus.getDescription());
                return;
            }

            lastMinuteMove.setMissionStatus(nextStatus);
            lastMinuteMoveRepository.save(lastMinuteMove);

            messagingTemplate.convertAndSend("/topic/mission/" + moveId + "/status", nextStatus.getDescription());
            logger.info("Statut de la mission LAST_MINUTE diffusé au client : moveId={}, statut={}", moveId, nextStatus.getDescription());

            Notification notification = new Notification();
            notification.setTitle("Mise à jour du statut de la mission LAST_MINUTE");
            notification.setNotificationType("MISSION_STATUS_UPDATE");
            notification.setRelatedMoveId(moveId);
            notification.setUserId(lastMinuteMove.getClient().getId());
            notification.setStatus("SENT");
            notification.setTimestamp(LocalDateTime.now());
            notification.setRead(false);
            try {
                notificationService.sendAndSaveNotification(notification, lastMinuteMove);
                logger.info("Notification de mise à jour du statut de la mission LAST_MINUTE envoyée pour le client : userId={}, moveId={}",
                        lastMinuteMove.getClient().getId(), moveId);
            } catch (Exception e) {
                logger.error("Échec de l'envoi de la notification de mise à jour du statut de la mission LAST_MINUTE : userId={}, moveId={}, erreur={}",
                        lastMinuteMove.getClient().getId(), moveId, e.getMessage(), e);
            }

            if (nextStatus == MissionStatus.COMPLETED) {
                lastMinuteMove.setAssignmentStatus("DRIVER_FREE");
                lastMinuteMoveRepository.save(lastMinuteMove);
                logger.info("Statut d'affectation du chauffeur {} défini à DRIVER_FREE après achèvement de la mission LAST_MINUTE", driverId);
                moveService.addOnlineDriver(driverId);
                taskScheduler.schedule(() -> moveService.assignNextPendingToDriver(driverId),
                        Instant.now().plusMillis(2000));
            }
            return;
        }

        logger.error("Mission non trouvée ni dans MoveRequest ni dans LastMinuteMove : {}", moveId);

        messagingTemplate.convertAndSend("/topic/mission/" + moveId + "/status", "Mission non trouvée");
        logger.info("Statut de mission non trouvée diffusé au client : moveId={}", moveId);
    }

    private void logDriverNotFound(String userId) {
        logger.warn("Chauffeur non trouvé pour l'ID : {}", userId);
    }
}